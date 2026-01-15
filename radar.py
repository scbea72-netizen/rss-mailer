import os
import time
import math
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

TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "").strip()
TG_CHAT_ID_US = (os.getenv("TG_CHAT_ID_US", "").strip() or os.getenv("TG_CHAT_ID", "").strip())
TG_CHAT_ID_JP = os.getenv("TG_CHAT_ID_JP", "").strip()
TG_CHAT_ID_KR = os.getenv("TG_CHAT_ID_KR", "").strip()

VOL_MULT = float(os.getenv("VOL_MULT", "1.5"))
RSI_MIN  = float(os.getenv("RSI_MIN", "50"))
SEND_EMPTY = os.getenv("SEND_EMPTY", "1").strip()
SEND_TEST  = os.getenv("SEND_TEST", "0").strip()

# 전종목 안정화
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
    if not TG_BOT_TOKEN:
        raise RuntimeError("TG_BOT_TOKEN이 비어있습니다.")
    if not chat_id:
        raise RuntimeError("TG_CHAT_ID가 비어있습니다.")
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    r = requests.post(url, data={"chat_id": chat_id, "text": text, "disable_web_page_preview": True}, timeout=25)
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
    if now.weekday() >= 5: return False
    t = now.time()
    return (t >= datetime.strptime("09:30", "%H:%M").time() and t <= datetime.strptime("16:00", "%H:%M").time())

def is_jp_market_open():
    now = datetime.now(JST)
    if now.weekday() >= 5: return False
    t = now.time()
    am = (t >= datetime.strptime("09:00", "%H:%M").time() and t <= datetime.strptime("11:30", "%H:%M").time())
    pm = (t >= datetime.strptime("12:30", "%H:%M").time() and t <= datetime.strptime("15:00", "%H:%M").time())
    return am or pm

def is_kr_market_open():
    now = datetime.now(KST)
    if now.weekday() >= 5: return False
    t = now.time()
    return (t >= datetime.strptime("09:00", "%H:%M").time() and t <= datetime.strptime("15:30", "%H:%M").time())

def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    up = delta.clip(lower=0)
    down = (-delta).clip(lower=0)
    ma_up = up.rolling(period).mean()
    ma_down = down.rolling(period).mean()
    rs = ma_up / ma_down.replace(0, pd.NA)
    return 100 - (100 / (1 + rs))

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
            if len(title) > 120: title = title[:120] + "…"
            if title: titles.append(title)
        return titles
    except Exception:
        return []

def download_batch(tickers: list[str], period: str, interval: str) -> dict[str, pd.DataFrame]:
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

    if not isinstance(df.columns, pd.MultiIndex):
        if {"Close","Volume"}.issubset(df.columns) and len(df) >= 30:
            out[tickers[0]] = df.dropna(subset=["Close","Volume"])
        return out

    for t in tickers:
        try:
            sub = df[t]
            if {"Close","Volume"}.issubset(sub.columns):
                sub = sub.dropna(subset=["Close","Volume"])
                if len(sub) >= 30:
                    out[t] = sub
        except Exception:
            continue
    return out

def scan_universe_batch(tickers: list[str], interval: str, period: str, market: str):
    hits = []
    if not tickers:
        return hits

    if MAX_TICKERS and MAX_TICKERS > 0:
        tickers = tickers[:MAX_TICKERS]

    batches = [tickers[i:i+BATCH_SIZE] for i in range(0, len(tickers), BATCH_SIZE)]

    for batch in batches:
        data_map = {}
        for attempt in range(RETRY + 1):
            try:
                data_map = download_batch(batch, period, interval)
                break
            except Exception:
                if attempt >= RETRY:
                    data_map = {}
                else:
                    time.sleep(0.8 + random.random())

        for t in batch:
            df = data_map.get(t)
            if df is None or df.empty:
                continue
            try:
                close = df["Close"]
                vol = df["Volume"]
                ma20 = close.rolling(20).mean()
                vol20 = vol.rolling(20).mean()
                r = rsi(close, 14)

                last_close = float(close.iloc[-1])
                prev_close = float(close.iloc[-2])
                last_ma20 = float(ma20.iloc[-1])
                prev_ma20 = float(ma20.iloc[-2])
                last_vol = float(vol.iloc[-1])
                last_vol20 = float(vol20.iloc[-1])
                last_rsi = float(r.iloc[-1])

                if last_vol20 == 0:
                    continue

                cross_up = (prev_close <= prev_ma20) and (last_close > last_ma20)
                vol_spike = last_vol >= (VOL_MULT * last_vol20)
                rsi_ok = last_rsi >= RSI_MIN

                if cross_up and vol_spike and rsi_ok:
                    hits.append({
                        "ticker": t,
                        "close": last_close,
                        "vol_mult": last_vol / last_vol20,
                        "rsi": last_rsi,
                        "ts_key": str(df.index[-1]),
                        "news": fetch_news_titles(t, market, 3),
                    })
            except Exception:
                continue

        time.sleep(max(0.0, SLEEP_BETWEEN_BATCH + random.random() * SLEEP_JITTER))

    hits.sort(key=lambda x: x["vol_mult"], reverse=True)
    return hits

def format_message(title, interval, hits):
    now = datetime.now(KST).strftime("%Y-%m-%d %H:%M")
    lines = [
        f"🚨 {title}",
        f"⏱ {interval} | 🕒 KST {now}",
        f"✅ 조건: 20MA 상향돌파 + 거래량 {VOL_MULT:.1f}x + RSI≥{RSI_MIN:.0f}",
        ""
    ]
    if not hits:
        lines.append("- 조건 충족 종목: 없음")
        return "\n".join(lines)

    for h in hits[:15]:
        lines.append(f"- {h['ticker']} | 종가 {h['close']:.2f} | 거래량 {h['vol_mult']:.1f}x | RSI {h['rsi']:.0f}")
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
    # ✅ 디버그 상태 리포트(스캔이 도는지, 티커가 비었는지 바로 확인)
    if os.getenv("DEBUG_STATUS", "0") == "1" and TG_CHAT_ID_KR:
        msg = (
            "📌 [KR 상태 리포트]\n"
            f"- tickers_kr.txt 개수: {len(kr_tickers)}\n"
            f"- MAX_TICKERS: {MAX_TICKERS}\n"
            f"- BATCH_SIZE: {BATCH_SIZE}\n"
            f"- VOL_MULT: {VOL_MULT}\n"
            f"- RSI_MIN: {RSI_MIN}\n"
        )
        # 앞 5개 티커도 같이 보여주기
        if kr_tickers:
            msg += "- 예시 티커(앞 5개): " + ", ".join(kr_tickers[:5])
        tg_send(TG_CHAT_ID_KR, msg)

    # ✅ 테스트
    if SEND_TEST == "1":
        now = datetime.now(KST).strftime("%Y-%m-%d %H:%M")
        if TG_CHAT_ID_US: tg_send(TG_CHAT_ID_US, f"✅ Radar 테스트(US) - {now}")
        if TG_CHAT_ID_JP: tg_send(TG_CHAT_ID_JP, f"✅ Radar 테스트(JP) - {now}")
        if TG_CHAT_ID_KR: tg_send(TG_CHAT_ID_KR, f"✅ Radar 테스트(KR) - {now}")

    # 🇺🇸 US
    if TG_CHAT_ID_US and us_tickers:
        if is_us_market_open():
            hits = scan_universe_batch(us_tickers, INTRADAY_INTERVAL, INTRADAY_PERIOD, "US")
            dedup_and_send("US", TG_CHAT_ID_US, INTRADAY_INTERVAL, "미국(장중) 20MA 돌파 + 거래량 폭증 + RSI + 뉴스", hits)
        else:
            hits = scan_universe_batch(us_tickers, DAILY_INTERVAL, DAILY_PERIOD, "US")
            dedup_and_send("US", TG_CHAT_ID_US, DAILY_INTERVAL, "미국(일봉) 20MA 돌파 + 거래량 폭증 + RSI + 뉴스", hits)

    # 🇯🇵 JP
    if TG_CHAT_ID_JP and jp_tickers:
        if is_jp_market_open():
            hits = scan_universe_batch(jp_tickers, INTRADAY_INTERVAL, INTRADAY_PERIOD, "JP")
            dedup_and_send("JP", TG_CHAT_ID_JP, INTRADAY_INTERVAL, "일본(장중) 20MA 돌파 + 거래량 폭증 + RSI + 뉴스", hits)
        else:
            hits = scan_universe_batch(jp_tickers, DAILY_INTERVAL, DAILY_PERIOD, "JP")
            dedup_and_send("JP", TG_CHAT_ID_JP, DAILY_INTERVAL, "일본(일봉) 20MA 돌파 + 거래량 폭증 + RSI + 뉴스", hits)

    # 🇰🇷 KR (코스피+코스닥 전종목)
    # ⚠️ kr_tickers가 비어있으면 알림이 아예 안 나가니까, 비어있을 때도 안내 메시지 보내게 처리
    if TG_CHAT_ID_KR:
        if not kr_tickers:
            if SEND_EMPTY == "1":
                tg_send(TG_CHAT_ID_KR, "⚠️ 한국 tickers_kr.txt가 비어있어서 스캔을 건너뜀 (update_tickers_kr.py 실행/휴장 여부 확인 필요)")
        else:
            if is_kr_market_open():
                hits = scan_universe_batch(kr_tickers, INTRADAY_INTERVAL, INTRADAY_PERIOD, "KR")
                dedup_and_send("KR", TG_CHAT_ID_KR, INTRADAY_INTERVAL, "한국(장중) 코스피+코스닥 전체 20MA 돌파 + 거래량 폭증 + RSI + 뉴스", hits)
            else:
                hits = scan_universe_batch(kr_tickers, DAILY_INTERVAL, DAILY_PERIOD, "KR")
                dedup_and_send("KR", TG_CHAT_ID_KR, DAILY_INTERVAL, "한국(일봉) 코스피+코스닥 전체 20MA 돌파 + 거래량 폭증 + RSI + 뉴스", hits)

    print("DONE")

if __name__ == "__main__":
    main()
