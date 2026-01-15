import os
import time
import json
import random
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

import requests
import feedparser
import yfinance as yf
import pandas as pd

KST = timezone(timedelta(hours=9))
JST = timezone(timedelta(hours=9))
ET  = timezone(timedelta(hours=-5))  # 단순 처리(서머타임은 무시)

# ✅ 승찬님 시크릿 그대로
TG_BOT_TOKEN  = os.getenv("TG_BOT_TOKEN", "").strip()
TG_CHAT_ID_US = os.getenv("TG_CHAT_ID_US", "").strip()
TG_CHAT_ID_JP = os.getenv("TG_CHAT_ID_JP", "").strip()
TG_CHAT_ID_KR = os.getenv("TG_CHAT_ID_KR", "").strip()

# ✅ 등락률 기준(거래량 제거)
PCT_MIN  = float(os.getenv("PCT_MIN", "3.0"))     # 예: 3.0 = +3% 이상
ABS_MODE = os.getenv("ABS_MODE", "0").strip()     # 1이면 |등락률| >= PCT_MIN (급등락 양방향)

# ✅ 스캔 파라미터
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "200"))
MAX_TICKERS = int(os.getenv("MAX_TICKERS", "4000"))
RETRY = int(os.getenv("RETRY", "2"))
SLEEP_BETWEEN_BATCH = float(os.getenv("SLEEP_BETWEEN_BATCH", "0.4"))

# 데이터 설정
INTRADAY_INTERVAL = "5m"
INTRADAY_PERIOD = "5d"
DAILY_INTERVAL = "1d"
DAILY_PERIOD = "10d"

US_TICKERS_FILE = "tickers_us.txt"
JP_TICKERS_FILE = "tickers_jp.txt"
KR_TICKERS_FILE = "tickers_kr.txt"

STATE_FILE = "state.json"


def tg_send(chat_id: str, text: str) -> None:
    if not TG_BOT_TOKEN:
        raise RuntimeError("TG_BOT_TOKEN missing")
    if not chat_id:
        raise RuntimeError("chat_id missing")
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    r = requests.post(
        url,
        data={"chat_id": chat_id, "text": text, "disable_web_page_preview": True},
        timeout=20
    )
    print("[TG] status:", r.status_code, "resp:", (r.text or "")[:200])
    r.raise_for_status()


def load_tickers(path: str) -> List[str]:
    if not os.path.exists(path):
        return []
    out = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            t = line.strip()
            if not t or t.startswith("#"):
                continue
            out.append(t.split()[0].strip())
    return out


def load_state() -> Dict:
    if not os.path.exists(STATE_FILE):
        return {"sent": {}}
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"sent": {}}


def save_state(state: Dict) -> None:
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def sig_key(market: str, ticker: str, interval: str, ts_key: str) -> str:
    return f"{market}|{ticker}|{interval}|{ts_key}"


def is_us_market_open() -> bool:
    now = datetime.now(ET)
    if now.weekday() >= 5:
        return False
    t = now.time()
    return (t >= datetime.strptime("09:30", "%H:%M").time() and t <= datetime.strptime("16:00", "%H:%M").time())


def is_jp_market_open() -> bool:
    now = datetime.now(JST)
    if now.weekday() >= 5:
        return False
    t = now.time()
    am = (t >= datetime.strptime("09:00", "%H:%M").time() and t <= datetime.strptime("11:30", "%H:%M").time())
    pm = (t >= datetime.strptime("12:30", "%H:%M").time() and t <= datetime.strptime("15:00", "%H:%M").time())
    return am or pm


def is_kr_market_open() -> bool:
    now = datetime.now(KST)
    if now.weekday() >= 5:
        return False
    t = now.time()
    return (t >= datetime.strptime("09:00", "%H:%M").time() and t <= datetime.strptime("15:30", "%H:%M").time())


def fetch_news_titles(query: str, market: str, limit: int = 2) -> List[str]:
    try:
        if market == "KR":
            url = f"https://news.google.com/rss/search?q={query}&hl=ko&gl=KR&ceid=KR:ko"
        elif market == "JP":
            url = f"https://news.google.com/rss/search?q={query}&hl=ja&gl=JP&ceid=JP:ja"
        else:
            url = f"https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"
        feed = feedparser.parse(url)
        titles = []
        for e in feed.entries[:limit]:
            title = (e.title or "").strip()
            if title:
                titles.append(title[:120])
        return titles
    except Exception:
        return []


def yf_download_batch(tickers: List[str], period: str, interval: str) -> Dict[str, pd.DataFrame]:
    out = {}
    if not tickers:
        return out

    df = yf.download(
        tickers=" ".join(tickers),
        period=period,
        interval=interval,
        group_by="ticker",
        threads=True,
        progress=False
    )
    if df is None or df.empty:
        return out

    # single ticker
    if not isinstance(df.columns, pd.MultiIndex):
        if "Close" in df.columns and len(df) >= 2:
            out[tickers[0]] = df.dropna(subset=["Close"])
        return out

    # multi
    for t in tickers:
        try:
            sub = df[t]
            if "Close" in sub.columns:
                sub = sub.dropna(subset=["Close"])
                if len(sub) >= 2:
                    out[t] = sub
        except Exception:
            continue
    return out


def yf_prev_close_map(tickers: List[str]) -> Dict[str, float]:
    """
    전일 종가 맵: 1d 10d 데이터에서 마지막-2 종가를 전일로 사용
    """
    prev_map: Dict[str, float] = {}
    if not tickers:
        return prev_map

    df = yf.download(
        tickers=" ".join(tickers),
        period="10d",
        interval="1d",
        group_by="ticker",
        threads=True,
        progress=False
    )
    if df is None or df.empty:
        return prev_map

    if not isinstance(df.columns, pd.MultiIndex):
        try:
            c = df["Close"].dropna()
            if len(c) >= 2:
                prev_map[tickers[0]] = float(c.iloc[-2])
        except Exception:
            pass
        return prev_map

    for t in tickers:
        try:
            sub = df[t]
            c = sub["Close"].dropna()
            if len(c) >= 2:
                prev_map[t] = float(c.iloc[-2])
        except Exception:
            continue
    return prev_map


def pct_change(last_price: float, base_price: float) -> Optional[float]:
    if not base_price or base_price == 0:
        return None
    return (last_price / base_price - 1.0) * 100.0


def scan_pct(
    tickers: List[str],
    market: str,
    market_open: bool
) -> List[Dict]:
    """
    ✅ 시장 열림: (마지막 5m close / 전일종가 - 1)*100
    ✅ 시장 닫힘: (오늘 종가 / 전일종가 - 1)*100 (일봉)
    """
    tickers = tickers[:MAX_TICKERS]
    hits: List[Dict] = []

    interval = INTRADAY_INTERVAL if market_open else DAILY_INTERVAL
    period = INTRADAY_PERIOD if market_open else DAILY_PERIOD

    batches = [tickers[i:i + BATCH_SIZE] for i in range(0, len(tickers), BATCH_SIZE)]

    for batch in batches:
        data_map = {}
        prev_map = {}

        for attempt in range(RETRY + 1):
            try:
                data_map = yf_download_batch(batch, period=period, interval=interval)
                prev_map = yf_prev_close_map(batch)  # 열림/닫힘 모두 전일종가 필요
                break
            except Exception:
                if attempt >= RETRY:
                    data_map = {}
                    prev_map = {}
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
                base = prev_map.get(t)
                if base is None:
                    continue

                pct = pct_change(last_price, base)
                if pct is None:
                    continue

                ok = (abs(pct) >= PCT_MIN) if ABS_MODE == "1" else (pct >= PCT_MIN)
                if ok:
                    hits.append({
                        "ticker": t,
                        "pct": pct,
                        "price": last_price,
                        "ts_key": str(df.index[-1]),
                        "news": fetch_news_titles(t, market, 2),
                    })
            except Exception:
                continue

        time.sleep(SLEEP_BETWEEN_BATCH)

    # 정렬
    if ABS_MODE == "1":
        hits.sort(key=lambda x: abs(x["pct"]), reverse=True)
    else:
        hits.sort(key=lambda x: x["pct"], reverse=True)

    return hits


def format_msg(title: str, interval: str, hits: List[Dict]) -> str:
    now = datetime.now(KST).strftime("%Y-%m-%d %H:%M")
    cond = f"|pct|≥{PCT_MIN:.1f}%" if ABS_MODE == "1" else f"+{PCT_MIN:.1f}% 이상"
    lines = [f"📈 {title}", f"⏱ {interval} | KST {now}", f"✅ 조건: 등락률 {cond}", ""]
    if not hits:
        lines.append("- 조건 충족 종목 없음")
        return "\n".join(lines)

    for h in hits[:15]:
        sign = "+" if h["pct"] >= 0 else ""
        lines.append(f"- {h['ticker']}  {sign}{h['pct']:.2f}%  (가격 {h['price']:.2f})")
        for nt in h.get("news", [])[:2]:
            lines.append(f"   • {nt}")
        lines.append("")
    return "\n".join(lines).strip()


def dedup_and_send(market: str, chat_id: str, interval: str, title: str, hits: List[Dict]) -> None:
    state = load_state()
    sent = state.setdefault("sent", {})

    new_hits = []
    for h in hits:
        k = sig_key(market, h["ticker"], interval, h["ts_key"])
        if sent.get(k):
            continue
        sent[k] = True
        new_hits.append(h)

    if new_hits:
        tg_send(chat_id, format_msg(title, interval, new_hits))

    save_state(state)


def main():
    us = load_tickers(US_TICKERS_FILE)
    jp = load_tickers(JP_TICKERS_FILE)
    kr = load_tickers(KR_TICKERS_FILE)

    # 🇺🇸 US
    if TG_CHAT_ID_US and us:
        open_ = is_us_market_open()
        hits = scan_pct(us, "US", market_open=open_)
        interval = INTRADAY_INTERVAL if open_ else DAILY_INTERVAL
        title = "미국(장중) 등락률 레이더" if open_ else "미국(일봉) 등락률 레이더"
        dedup_and_send("US", TG_CHAT_ID_US, interval, title, hits)

    # 🇯🇵 JP
    if TG_CHAT_ID_JP and jp:
        open_ = is_jp_market_open()
        hits = scan_pct(jp, "JP", market_open=open_)
        interval = INTRADAY_INTERVAL if open_ else DAILY_INTERVAL
        title = "일본(장중) 등락률 레이더" if open_ else "일본(일봉) 등락률 레이더"
        dedup_and_send("JP", TG_CHAT_ID_JP, interval, title, hits)

    # 🇰🇷 KR
    if TG_CHAT_ID_KR:
        if not kr:
            tg_send(TG_CHAT_ID_KR, "⚠️ tickers_kr.txt가 비어있습니다. (한국 전종목 티커 파일부터 채워야 함)")
        else:
            open_ = is_kr_market_open()
            hits = scan_pct(kr, "KR", market_open=open_)
            interval = INTRADAY_INTERVAL if open_ else DAILY_INTERVAL
            title = "한국(장중) 등락률 레이더" if open_ else "한국(일봉) 등락률 레이더"
            dedup_and_send("KR", TG_CHAT_ID_KR, interval, title, hits)

    print("DONE")


if __name__ == "__main__":
    main()
