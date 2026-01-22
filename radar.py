import os
import time
import json
import random
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import requests
import feedparser
import yfinance as yf
import pandas as pd

# ✅ DST/시간대 자동 처리
from zoneinfo import ZoneInfo

# -----------------------------
# Timezones (DST safe)
# -----------------------------
KST = ZoneInfo("Asia/Seoul")
JST = ZoneInfo("Asia/Tokyo")
ET  = ZoneInfo("America/New_York")  # ✅ DST 자동 반영

# -----------------------------
# ENV
# -----------------------------
TG_BOT_TOKEN  = os.getenv("TG_BOT_TOKEN", "").strip()
TG_CHAT_ID_US = os.getenv("TG_CHAT_ID_US", "").strip()
TG_CHAT_ID_JP = os.getenv("TG_CHAT_ID_JP", "").strip()
TG_CHAT_ID_KR = os.getenv("TG_CHAT_ID_KR", "").strip()

PCT_MIN  = float(os.getenv("PCT_MIN", "3.0"))
ABS_MODE = os.getenv("ABS_MODE", "0").strip()  # 1이면 |등락률| >= PCT_MIN

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "200"))
MAX_TICKERS = int(os.getenv("MAX_TICKERS", "4000"))  # 0이면 전체
RETRY = int(os.getenv("RETRY", "2"))
SLEEP_BETWEEN_BATCH = float(os.getenv("SLEEP_BETWEEN_BATCH", "0.4"))

INTRADAY_INTERVAL = "5m"
INTRADAY_PERIOD = "5d"
DAILY_INTERVAL = "1d"
DAILY_PERIOD = "10d"

US_TICKERS_FILE = "tickers_us.txt"
JP_TICKERS_FILE = "tickers_jp.txt"
KR_TICKERS_FILE = "tickers_kr.txt"

STATE_FILE = "state.json"

TICKER_NAMES_FILE = "ticker_names.json"
TICKER_NAME_MAX_FETCH = int(os.getenv("TICKER_NAME_MAX_FETCH", "300"))

# ✅ 텔레그램 메시지/전송 안정화
TG_MAX_LEN = int(os.getenv("TG_MAX_LEN", "3800"))
TOP_N = int(os.getenv("TOP_N", "30"))
NEWS_PER_TICKER = int(os.getenv("NEWS_PER_TICKER", "2"))

TG_SEND_MIN_INTERVAL = float(os.getenv("TG_SEND_MIN_INTERVAL", "0.8"))
TG_RETRY_429 = int(os.getenv("TG_RETRY_429", "6"))
TG_RETRY_BASE_SLEEP = float(os.getenv("TG_RETRY_BASE_SLEEP", "1.5"))

_last_tg_send_ts = 0.0

# -----------------------------
# (Optional) Market calendars (휴장일/정규장 정확 판정)
# -----------------------------
# exchange_calendars 가 설치돼 있으면 사용
# - NYSE: "XNYS"
# - Tokyo: "XTKS"
# - Korea Exchange: "XKRX"
_CAL_AVAILABLE = False
try:
    import exchange_calendars as xcals
    _CAL_US = xcals.get_calendar("XNYS")
    _CAL_JP = xcals.get_calendar("XTKS")
    _CAL_KR = xcals.get_calendar("XKRX")
    _CAL_AVAILABLE = True
except Exception:
    _CAL_AVAILABLE = False
    _CAL_US = _CAL_JP = _CAL_KR = None


# -----------------------------
# Telegram helpers
# -----------------------------
def _split_message(text: str, max_len: int = TG_MAX_LEN) -> List[str]:
    if text is None:
        return []
    text = str(text).strip()
    if not text:
        return []
    if len(text) <= max_len:
        return [text]

    chunks: List[str] = []
    lines = text.splitlines()
    buf: List[str] = []
    cur = 0

    for line in lines:
        add = (1 if buf else 0) + len(line)
        if cur + add <= max_len:
            buf.append(line)
            cur += add
            continue

        if buf:
            chunks.append("\n".join(buf).strip())
            buf, cur = [], 0

        if len(line) > max_len:
            s = line
            while len(s) > max_len:
                chunks.append(s[:max_len].strip())
                s = s[max_len:]
            if s.strip():
                buf = [s.strip()]
                cur = len(buf[0])
        else:
            buf = [line]
            cur = len(line)

    if buf:
        chunks.append("\n".join(buf).strip())

    return [c for c in chunks if c and c.strip()]


def tg_send(chat_id: str, text: str) -> None:
    global _last_tg_send_ts

    if not TG_BOT_TOKEN:
        raise RuntimeError("TG_BOT_TOKEN missing")
    if not chat_id:
        raise RuntimeError("chat_id missing")

    parts = _split_message(text, TG_MAX_LEN)
    if not parts:
        print("[TG] skip: empty text")
        return

    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"

    for i, part in enumerate(parts, 1):
        now = time.time()
        gap = now - _last_tg_send_ts
        if gap < TG_SEND_MIN_INTERVAL:
            time.sleep(TG_SEND_MIN_INTERVAL - gap)

        payload = {
            "chat_id": chat_id,
            "text": part,
            "disable_web_page_preview": True,
        }

        attempt = 0
        while True:
            attempt += 1
            try:
                r = requests.post(url, data=payload, timeout=20)
                print(f"[TG] part {i}/{len(parts)} try {attempt} status:", r.status_code, "resp:", (r.text or "")[:200])

                if r.status_code == 429 and attempt <= TG_RETRY_429:
                    retry_after = None
                    try:
                        j = r.json()
                        retry_after = (j.get("parameters") or {}).get("retry_after")
                    except Exception:
                        retry_after = None

                    sleep_s = float(retry_after) if retry_after else (TG_RETRY_BASE_SLEEP * (2 ** (attempt - 1)))
                    sleep_s = min(sleep_s, 60.0)
                    print(f"[TG] 429 rate limited. sleep {sleep_s:.1f}s then retry...")
                    time.sleep(sleep_s)
                    continue

                r.raise_for_status()
                _last_tg_send_ts = time.time()
                break

            except requests.RequestException as e:
                if attempt <= 2:
                    time.sleep(0.8 * attempt)
                    continue
                try:
                    resp_text = getattr(e.response, "text", None)
                    if resp_text:
                        print("[TG] error response:", resp_text[:500])
                except Exception:
                    pass
                raise


# -----------------------------
# State / tickers
# -----------------------------
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


# -----------------------------
# Market open/close 판단 (DST+휴장)
# -----------------------------
def _fallback_open_status(market: str) -> Tuple[bool, str]:
    """
    라이브러리 없을 때: 요일+시간 기반.
    DST는 ZoneInfo로 해결됨.
    """
    if market == "US":
        now = datetime.now(ET)
        if now.weekday() >= 5:
            return False, f"US CLOSED (weekend) ET {now:%Y-%m-%d %H:%M}"
        t = now.time()
        open_ = (t >= datetime.strptime("09:30", "%H:%M").time() and
                 t <= datetime.strptime("16:00", "%H:%M").time())
        return open_, f"US {'OPEN' if open_ else 'CLOSED'} (fallback) ET {now:%Y-%m-%d %H:%M}"

    if market == "JP":
        now = datetime.now(JST)
        if now.weekday() >= 5:
            return False, f"JP CLOSED (weekend) JST {now:%Y-%m-%d %H:%M}"
        t = now.time()
        am = (t >= datetime.strptime("09:00", "%H:%M").time() and t <= datetime.strptime("11:30", "%H:%M").time())
        pm = (t >= datetime.strptime("12:30", "%H:%M").time() and t <= datetime.strptime("15:00", "%H:%M").time())
        open_ = am or pm
        return open_, f"JP {'OPEN' if open_ else 'CLOSED'} (fallback) JST {now:%Y-%m-%d %H:%M}"

    # KR
    now = datetime.now(KST)
    if now.weekday() >= 5:
        return False, f"KR CLOSED (weekend) KST {now:%Y-%m-%d %H:%M}"
    t = now.time()
    open_ = (t >= datetime.strptime("09:00", "%H:%M").time() and
             t <= datetime.strptime("15:30", "%H:%M").time())
    return open_, f"KR {'OPEN' if open_ else 'CLOSED'} (fallback) KST {now:%Y-%m-%d %H:%M}"


def market_status(market: str) -> Tuple[bool, str]:
    """
    가능하면 exchange_calendars로 '정규장 + 휴장일'까지 정확하게 판정.
    없으면 fallback.
    """
    if not _CAL_AVAILABLE:
        return _fallback_open_status(market)

    try:
        if market == "US":
            cal = _CAL_US
            now = datetime.now(ET)
        elif market == "JP":
            cal = _CAL_JP
            now = datetime.now(JST)
        else:
            cal = _CAL_KR
            now = datetime.now(KST)

        # exchange_calendars는 timezone-aware datetime 받으면 내부적으로 처리 가능.
        is_open = cal.is_open_on_minute(now)

        # 오늘 세션 정보(휴장일이면 없을 수 있음)
        # now.date() 기준으로 세션 조회
        # 세션이 없는 날(휴장일)에는 바로 CLOSED 표시
        sessions = cal.sessions_in_range(pd.Timestamp(now.date()), pd.Timestamp(now.date()))
        if sessions.empty:
            return False, f"{market} CLOSED (holiday) local {now:%Y-%m-%d %H:%M}"

        # 장 시작/끝(UTC -> local)
        session = sessions[0]
        open_utc = cal.session_open(session)
        close_utc = cal.session_close(session)

        # local 변환
        tz = ET if market == "US" else (JST if market == "JP" else KST)
        open_local = open_utc.tz_convert(tz)
        close_local = close_utc.tz_convert(tz)

        return bool(is_open), (
            f"{market} {'OPEN' if is_open else 'CLOSED'} "
            f"(calendar) local {now:%Y-%m-%d %H:%M} | "
            f"open {open_local:%H:%M} close {close_local:%H:%M}"
        )
    except Exception as e:
        # 캘린더가 문제 생기면 fallback
        open_, desc = _fallback_open_status(market)
        return open_, f"{desc} | calendar_error={type(e).__name__}"


# -----------------------------
# News
# -----------------------------
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


# -----------------------------
# YFinance helpers
# -----------------------------
def yf_download_batch(tickers: List[str], period: str, interval: str) -> Dict[str, pd.DataFrame]:
    out: Dict[str, pd.DataFrame] = {}
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


# -----------------------------
# Name cache
# -----------------------------
def load_ticker_names() -> Dict[str, str]:
    if not os.path.exists(TICKER_NAMES_FILE):
        return {}
    try:
        with open(TICKER_NAMES_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict):
                return {k: str(v) for k, v in data.items() if v}
    except Exception:
        pass
    return {}


def save_ticker_names(names: Dict[str, str]) -> None:
    try:
        with open(TICKER_NAMES_FILE, "w", encoding="utf-8") as f:
            json.dump(names, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def fetch_name_yf(ticker: str) -> Optional[str]:
    try:
        info = yf.Ticker(ticker).info or {}
        name = info.get("shortName") or info.get("longName") or info.get("displayName")
        if name:
            name = str(name).strip()
            if len(name) > 60:
                name = name[:60] + "…"
            return name
    except Exception:
        return None
    return None


def ensure_names_for(tickers: List[str], names_cache: Dict[str, str]) -> Dict[str, str]:
    missing = [t for t in tickers if t not in names_cache]
    if not missing:
        return names_cache

    to_fetch = missing[:TICKER_NAME_MAX_FETCH]
    added = 0
    for t in to_fetch:
        nm = fetch_name_yf(t)
        if nm:
            names_cache[t] = nm
            added += 1
        time.sleep(0.05)

    if added > 0:
        save_ticker_names(names_cache)
        print(f"[NAME] added {added} names (cache size={len(names_cache)})")
    return names_cache


def get_display_name(ticker: str, names_cache: Dict[str, str]) -> str:
    nm = names_cache.get(ticker, "")
    if not nm:
        return ticker
    return f"{ticker} ({nm})"


# -----------------------------
# Scanner
# -----------------------------
def scan_pct(tickers: List[str], market: str, market_open: bool) -> List[Dict]:
    if MAX_TICKERS and MAX_TICKERS > 0:
        tickers = tickers[:MAX_TICKERS]

    hits: List[Dict] = []

    interval = INTRADAY_INTERVAL if market_open else DAILY_INTERVAL
    period = INTRADAY_PERIOD if market_open else DAILY_PERIOD

    batches = [tickers[i:i + BATCH_SIZE] for i in range(0, len(tickers), BATCH_SIZE)]

    for batch in batches:
        data_map: Dict[str, pd.DataFrame] = {}
        prev_map: Dict[str, float] = {}

        for attempt in range(RETRY + 1):
            try:
                data_map = yf_download_batch(batch, period=period, interval=interval)
                prev_map = yf_prev_close_map(batch)
                break
            except Exception:
                if attempt >= RETRY:
                    data_map, prev_map = {}, {}
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
                        "news": fetch_news_titles(t, market, max(0, NEWS_PER_TICKER)),
                    })
            except Exception:
                continue

        time.sleep(SLEEP_BETWEEN_BATCH)

    hits.sort(key=(lambda x: abs(x["pct"])) if ABS_MODE == "1" else (lambda x: x["pct"]), reverse=True)
    return hits


def format_msg(title: str, interval: str, hits: List[Dict], names_cache: Dict[str, str], status_line: str) -> str:
    now_kst = datetime.now(KST).strftime("%Y-%m-%d %H:%M")
    cond = f"|pct|≥{PCT_MIN:.1f}%" if ABS_MODE == "1" else f"+{PCT_MIN:.1f}% 이상"

    lines = [
        f"📈 {title}",
        f"🕒 {status_line}",
        f"⏱ interval={interval} | KST {now_kst}",
        f"✅ 조건: 등락률 {cond}",
        ""
    ]

    if not hits:
        lines.append("- 조건 충족 종목 없음")
        return "\n".join(lines)

    for h in hits[:max(1, TOP_N)]:
        sign = "+" if h["pct"] >= 0 else ""
        disp = get_display_name(h["ticker"], names_cache)
        lines.append(f"- {disp}  {sign}{h['pct']:.2f}%  (가격 {h['price']:.2f})")
        for nt in h.get("news", [])[:max(0, NEWS_PER_TICKER)]:
            lines.append(f"   • {nt}")
        lines.append("")

    return "\n".join(lines).strip()


def dedup_and_send(market: str, chat_id: str, interval: str, title: str, hits: List[Dict], names_cache: Dict[str, str], status_line: str) -> None:
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
        msg = format_msg(title, interval, new_hits, names_cache, status_line)
        tg_send(chat_id, msg)

    save_state(state)


# -----------------------------
# Main
# -----------------------------
def main():
    us = load_tickers(US_TICKERS_FILE)
    jp = load_tickers(JP_TICKERS_FILE)
    kr = load_tickers(KR_TICKERS_FILE)

    names_cache = load_ticker_names()
    all_tickers = (us or []) + (jp or []) + (kr or [])
    names_cache = ensure_names_for(all_tickers, names_cache)

    # 🇺🇸 US
    if TG_CHAT_ID_US and us:
        open_, status_line = market_status("US")
        hits = scan_pct(us, "US", market_open=open_)
        interval = INTRADAY_INTERVAL if open_ else DAILY_INTERVAL
        title = "미국 등락률 레이더(장중)" if open_ else "미국 등락률 레이더(장마감/휴장)"
        dedup_and_send("US", TG_CHAT_ID_US, interval, title, hits, names_cache, status_line)

    # 🇯🇵 JP
    if TG_CHAT_ID_JP and jp:
        open_, status_line = market_status("JP")
        hits = scan_pct(jp, "JP", market_open=open_)
        interval = INTRADAY_INTERVAL if open_ else DAILY_INTERVAL
        title = "일본 등락률 레이더(장중)" if open_ else "일본 등락률 레이더(장마감/휴장)"
        dedup_and_send("JP", TG_CHAT_ID_JP, interval, title, hits, names_cache, status_line)

    # 🇰🇷 KR
    if TG_CHAT_ID_KR:
        if not kr:
            tg_send(TG_CHAT_ID_KR, "⚠️ tickers_kr.txt가 비어있습니다. (한국 전종목 티커 파일부터 채워야 함)")
        else:
            open_, status_line = market_status("KR")
            hits = scan_pct(kr, "KR", market_open=open_)
            interval = INTRADAY_INTERVAL if open_ else DAILY_INTERVAL
            title = "한국 등락률 레이더(장중)" if open_ else "한국 등락률 레이더(장마감/휴장)"
            dedup_and_send("KR", TG_CHAT_ID_KR, interval, title, hits, names_cache, status_line)

    print(f"[INFO] calendar_available={_CAL_AVAILABLE}")
    print("DONE")


if __name__ == "__main__":
    main()
