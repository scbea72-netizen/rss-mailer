import os
import time
import json
import random
import requests
import urllib.parse
import feedparser
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta, timezone

KST = timezone(timedelta(hours=9))
JST = timezone(timedelta(hours=9))
ET  = timezone(timedelta(hours=-5))  # 단순화

# ✅ 텔레그램(기존 변수명 유지)
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "").strip()
TG_CHAT_ID_US = (os.getenv("TG_CHAT_ID_US", "").strip() or os.getenv("TG_CHAT_ID", "").strip())
TG_CHAT_ID_JP = os.getenv("TG_CHAT_ID_JP", "").strip()
TG_CHAT_ID_KR = os.getenv("TG_CHAT_ID_KR", "").strip()

# ✅ 등락률 기준(거래량/RSI 완전 제거)
PCT_MIN   = float(os.getenv("PCT_MIN", "3.0"))   # 예: 3.0 = +3% 이상
ABS_MODE  = os.getenv("ABS_MODE", "0").strip()   # 1이면 |등락률| >= PCT_MIN (급등락 양방향)

SEND_EMPTY = os.getenv("SEND_EMPTY", "1").strip()
SEND_TEST  = os.getenv("SEND_TEST", "0").strip()

# 전종목 안정화(기존 유지)
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "200"))
MAX_TICKERS = int(os.getenv("MAX_TICKERS", "0"))
RETRY = int(os.getenv("RETRY", "2"))
SLEEP_BETWEEN_BATCH = float(os.getenv("SLEEP_BETWEEN_BATCH", "0.6"))
SLEEP_JITTER = float(os.getenv("SLEEP_JITTER", "0.4"))

INTRADAY_INTERVAL = "5m"
INTRADAY_PERIOD   = "5d"
DAILY_INTERVAL    = "1d"
DAILY_PERIOD      = "6mo"

US_TICKERS_FILE = "tickers_us.txt"
JP_TICKERS_FILE = "tickers_jp.txt"
KR_TICKERS_FILE = "tickers_kr.txt"
STATE_FILE = "state.json"


def tg_send(chat_id: str, text: str):
    """✅ 실패 원인 로그를 남기고, 필요 이상으로 죽지 않게 처리"""
    if not TG_BOT_TOKEN:
        raise RuntimeError("TG_BOT_TOKEN이 비어있습니다.")
    if not chat_id:
        raise RuntimeError("TG_CHAT_ID가 비어있습니다.")
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    r = requests.post(
        url,
        data={"chat_id": chat_id, "text": text, "disable_web_page_preview": True},
        timeout=25
    )
    # 로그 남김(403/400 바로 확인용)
    print("[TG] status:", r.status_code, "resp:", (r.text or "")[:250])
    if r.status_code != 200:
        raise RuntimeError(f"Telegram send failed {r.status_code}: {r.text[:300]}")


def load_tickers(path: str):
    if not os.path.exists(path):
        return []
    out = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            t = line.strip()
            if not t or t.startswith("#"):
                continue
            t = t.split()[0].strip()
            if t:
                out.append(t)
    return out


def load_state():
    if not os.path.exists(STATE_FILE):
        return {"sent": {}}
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"sent": {}}


def save_state(state):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def sig_key(market: str, ticker: str, interval: str, ts_key: str):
    return f"{market}|{ticker}|{interval}|{ts_key}"


def is_us_market_open():
    now = datetime.now(ET)
    if now.weekday() >= 5:
        return False
    t = now.time()
    return (t >= datetime.strptime("09:30", "%H:%M").time() and t <= datetime.strptime("16:00", "%H:%M").time())


def is_jp_market_open():
    now = datetime.now(JST)
    if now.weekday() >= 5:
        return False
    t = now.time()
    am = (t >= datetime.strptime("09:00", "%H:%M").time() and t <= datetime.strptime("11:30", "%H:%M").time())
    pm = (t >= datetime.strptime("12:30", "%H:%M").time() and t <= datetime.strptime("15:00", "%H:%M").time())
    return am or pm


def is_kr_market_open():
    now = datetime.now(KST)
    if now.weekday() >= 5:
        return False
    t = now.time()
    return (t >= datetime.strptime("09:00", "%H:%M").time() and t <= datetime.strptime("15:30", "%H:%M").time())


def fetch_news_titles(query: str, market: str, limit: int = 3):
    try:
        q = urllib.parse.quote(query)
        if market == "JP":
            url = f"https://news.google.com/rss/search?q={q}&hl=ja&gl=JP&ceid=JP:ja"
        elif market == "KR":
            url = f"https://news.google.com/rss/search?q={q}&hl=ko&gl=KR&ceid=KR:ko"
        else:
            url = f"https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en"
        feed = feedparser.parse(url)
        titles = []
        for e in feed.entries[:limit]:
            title = (e.title or "").strip()
            if len(title) > 120:
                title = title[:120] + "…"
            if title:
                titles.append(title)
        return titles
    except Exception:
        return []


def download_batch(tickers: list[str], period: str, interval: str) -> dict[str, pd.DataFrame]:
    """
    yfinance batch 다운로드:
    - intraday: interval=5m, period=5d
    - daily: interval=1d, period=6mo
    """
    out = {}
    if not tickers:
        return out

    df = yf.download(
        tickers=" ".join(tickers),
        period=period,
        interval=interval,
        group_by="ticker",
        auto_adjust=False,
        threads=True,
        progress=False,
    )
    if df is None or df.empty:
        return out

    # 단일 티커
    if not isinstance(df.columns, pd.MultiIndex):
        if {"Close"}.issubset(df.columns) and len(df) >= 2:
            out[tickers[0]] = df.dropna(subset=["Close"])
        return out

    # 멀티 티커
    for t in tickers:
        try:
            sub = df[t]
            if {"Close"}.issubset(sub.columns):
                sub = sub.dropna(subset=["Close"])
                if len(sub) >= 2:
                    out[t] = sub
        except Exception:
            continue
    return out


def download_prev_close_map(tickers: list[str]) -> dict[str, float]:
    """
    ✅ 장중 등락률 계산용: 전일 종가(prev_close) 맵
    - daily 1d로 5d 받아서 마지막 2개 일봉 종가로 전일종가 추출
    """
    prev_map: dict[str, float] = {}
    if not tickers:
        return prev_map

    df = yf.download(
        tickers=" ".join(tickers),
        period="10d",
        interval="1d",
        group_by="ticker",
        auto_adjust=False,
        threads=True,
        progress=False,
    )
    if df is None or df.empty:
        return prev_map

    if not isinstance(df.columns, pd.MultiIndex):
        try:
            close = df["Close"].dropna()
            if len(close) >= 2:
                prev_map[tickers[0]] = float(close.iloc[-2])
        except Exception:
            pass
        return prev_map

    for t in tickers:
        try:
            sub = df[t]
            close = sub["Close"].dropna()
            if len(close) >= 2:
                prev_map[t] = float(close.iloc[-2])
        except Exception:
            continue
    return prev_map


def compute_pct_change(last_price: float, base_price: float) -> float | None:
    if base_price is None or base_price == 0:
        return None
    return (last_price / base_price - 1.0) * 100.0


def scan_universe_batch_pct(
    tickers: list[str],
    interval: str,
    period: str,
    market: str,
    market_open: bool
):
    """
    ✅ 거래량/RSI/20MA 제거
    ✅ 등락률(%) 기준:
      - 시장 열림: (현재가(마지막 5m close) / 전일종가 - 1)*100
      - 시장 닫힘: (오늘 종가 / 전일 종가 - 1)*100
    """
    hits = []
    if not tickers:
        return hits

    if MAX_TICKERS and MAX_TICKERS > 0:
        tickers = tickers[:MAX_TICKERS]

    batches = [tickers[i:i + BATCH_SIZE] for i in range(0, len(tickers), BATCH_SIZE)]

    for batch in batches:
        data_map = {}
        prev_close_map = {}

        for attempt in range(RETRY + 1):
            try:
                # intraday or daily close series
                data_map = download_batch(batch, period, interval)
                # 장중이면 전일종가도 같이 가져옴
                if market_open:
                    prev_close_map = download_prev_close_map(batch)
                break
            except Exception:
                if attempt >= RETRY:
                    data_map = {}
                    prev_close_map = {}
                else:
                    time.sleep(0.8 + random.random())

        for t in batch:
            df = data_map.get(t)
            if df is None or df.empty:
                continue

            try:
                close = df["Close"].dropna()
                if len(close) < 2:
                    continue

                last_price = float(close.iloc[-1])

                if market_open:
                    # 장중: 전일 종가 기준
                    base = prev_close_map.get(t)
                    if base is None:
                        # fallback: intraday 데이터의 "직전 값" 기준(최후의 fallback)
                        base = float(close.iloc[0])
                    pct = compute_pct_change(last_price, base)
                    ts_key = str(df.index[-1])  # 마지막 바 timestamp
                else:
                    # 장마감: 일봉 기준 전일 종가 대비
                    prev_close = float(close.iloc[-2])
                    pct = compute_pct_change(last_price, prev_close)
                    ts_key = str(df.index[-1])

                if pct is None:
                    continue

                # 조건 판정
                ok = (abs(pct) >= PCT_MIN) if ABS_MODE == "1" else (pct >= PCT_MIN)
                if ok:
                    hits.append({
                        "ticker": t,
                        "price": last_price,
                        "pct": pct,
                        "ts_key": ts_key,
                        "news": fetch_news_titles(t, market, 3),
                    })
            except Exception:
                continue

        time.sleep(max(0.0, SLEEP_BETWEEN_BATCH + random.random() * SLEEP_JITTER))

    # 정렬: ABS_MODE면 절대값 큰 순, 아니면 상승률 큰 순
    if ABS_MODE == "1":
        hits.sort(key=lambda x: abs(x["pct"]), reverse=True)
    else:
        hits.sort(key=lambda x: x["pct"], reverse=True)

    return hits


def format_message(title, interval, hits):
    now = datetime.now(KST).strftime("%Y-%m-%d %H:%M")
    mode_txt = f"|pct|≥{PCT_MIN:.1f}%" if ABS_MODE == "1" else f"+{PCT_MIN:.1f}% 이상"
    lines = [
        f"🚨 {title}",
        f"⏱ {interval} | 🕒 KST {now}",
        f"✅ 조건: 등락률 {mode_txt}",
        ""
    ]
    if not hits:
        lines.append("- 조건 충족 종목: 없음")
        return "\n".join(lines)

    for h in hits[:15]:
        sign = "+" if h["pct"] >= 0 else ""
        lines.append(f"- {h['ticker']} | 가격 {h['price']:.2f} | 등락률 {sign}{h['pct']:.2f}%")
        if h["news"]:
            for nt in h["news"]:
                lines.append(f"   • {nt}")
        lines.append("")
    return "\n".join(lines).strip()


def dedup_and_send(market, chat_id, interval, title, hits):
    state = load_state()
    sent = state.setdefault("sent", {})

    new_hits = []
    for h in hits:
        k = sig_key(market, h["ticker"], interval, h["ts_key"])
        if sent.get(k):
            continue
        sent[k] = True
        new_hits.append(h)

    if not hits:
        if SEND_EMPTY == "1":
            tg_send(chat_id, format_message(title, interval, []))
    else:
        if new_hits:
            tg_send(chat_id, format_message(title, interval, new_hits))

    save_state(state)


def main():
    us_tickers = load_tickers(US_TICKERS_FILE)
    jp_tickers = load_tickers(JP_TICKERS_FILE)
    kr_tickers = load_tickers(KR_TICKERS_FILE)

    # ✅ 디버그 상태 리포트(티커 비었는지 바로 확인)
    if os.getenv("DEBUG_STATUS", "0") == "1" and TG_CHAT_ID_KR:
        msg = (
            "📌 [KR 상태 리포트]\n"
            f"- tickers_kr.txt 개수: {len(kr_tickers)}\n"
            f"- MAX_TICKERS: {MAX_TICKERS}\n"
            f"- BATCH_SIZE: {BATCH_SIZE}\n"
            f"- PCT_MIN: {PCT_MIN}\n"
            f"- ABS_MODE: {ABS_MODE}\n"
        )
        if kr_tickers:
            msg += "- 예시 티커(앞 5개): " + ", ".join(kr_tickers[:5])
        tg_send(TG_CHAT_ID_KR, msg)

    # ✅ 텔레그램 테스트(필요하면 workflow에서 SEND_TEST=1로 한번 실행)
    if SEND_TEST == "1":
        now = datetime.now(KST).strftime("%Y-%m-%d %H:%M")
        if TG_CHAT_ID_US:
            tg_send(TG_CHAT_ID_US, f"✅ Radar 테스트(US) - {now}")
        if TG_CHAT_ID_JP:
            tg_send(TG_CHAT_ID_JP, f"✅ Radar 테스트(JP) - {now}")
        if TG_CHAT_ID_KR:
            tg_send(TG_CHAT_ID_KR, f"✅ Radar 테스트(KR) - {now}")

    # 🇺🇸 US
    if TG_CHAT_ID_US and us_tickers:
        if is_us_market_open():
            hits = scan_universe_batch_pct(us_tickers, INTRADAY_INTERVAL, INTRADAY_PERIOD, "US", market_open=True)
            dedup_and_send("US", TG_CHAT_ID_US, INTRADAY_INTERVAL, "미국(장중) 등락률 레이더 + 뉴스", hits)
        else:
            hits = scan_universe_batch_pct(us_tickers, DAILY_INTERVAL, DAILY_PERIOD, "US", market_open=False)
            dedup_and_send("US", TG_CHAT_ID_US, DAILY_INTERVAL, "미국(일봉) 등락률 레이더 + 뉴스", hits)

    # 🇯🇵 JP
    if TG_CHAT_ID_JP and jp_tickers:
        if is_jp_market_open():
            hits = scan_universe_batch_pct(jp_tickers, INTRADAY_INTERVAL, INTRADAY_PERIOD, "JP", market_open=True)
            dedup_and_send("JP", TG_CHAT_ID_JP, INTRADAY_INTERVAL, "일본(장중) 등락률 레이더 + 뉴스", hits)
        else:
            hits = scan_universe_batch_pct(jp_tickers, DAILY_INTERVAL, DAILY_PERIOD, "JP", market_open=False)
            dedup_and_send("JP", TG_CHAT_ID_JP, DAILY_INTERVAL, "일본(일봉) 등락률 레이더 + 뉴스", hits)

    # 🇰🇷 KR
    if TG_CHAT_ID_KR:
        if not kr_tickers:
            if SEND_EMPTY == "1":
                tg_send(TG_CHAT_ID_KR, "⚠️ 한국 tickers_kr.txt가 비어있어서 스캔을 건너뜀 (티커 파일 생성/업데이트 필요)")
        else:
            if is_kr_market_open():
                hits = scan_universe_batch_pct(kr_tickers, INTRADAY_INTERVAL, INTRADAY_PERIOD, "KR", market_open=True)
                dedup_and_send("KR", TG_CHAT_ID_KR, INTRADAY_INTERVAL, "한국(장중)
