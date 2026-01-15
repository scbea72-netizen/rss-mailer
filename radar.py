import os
import time
import math
import json
import requests
import yfinance as yf
import pandas as pd
from datetime import datetime, timezone, timedelta

# ===== Timezone =====
KST = timezone(timedelta(hours=9))

# ===== ENV (필수) =====
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "").strip()

# ✅ 호환: 둘 중 아무거나 들어와도 동작하게
TG_CHAT_ID_US = (os.getenv("TG_CHAT_ID_US", "").strip()
                 or os.getenv("TG_CHAT_ID", "").strip())
TG_CHAT_ID_JP = (os.getenv("TG_CHAT_ID_JP", "").strip()
                 or os.getenv("TG_CHAT_ID_JP_ALT", "").strip())

# ===== ENV (옵션: 기준 튜닝) =====
VOL_MULT = float(os.getenv("VOL_MULT", "2.0"))           # 거래량 폭증 배수
MIN_CHANGE_PCT = float(os.getenv("MIN_CHANGE_PCT", "0")) # 전일대비 상승률 최소(%)
INTERVAL = os.getenv("INTERVAL", "1d")
PERIOD = os.getenv("PERIOD", "6mo")

# ✅ 테스트 메시지(연결 확인용) (기본 OFF로 바꿈: 원하면 Actions env에서 "1"로 켜세요)
SEND_TEST = os.getenv("SEND_TEST", "0").strip()          # "1"=보냄, "0"=안보냄

# ===== Ticker file paths (레포 루트에 만들어둔 txt) =====
US_TICKERS_FILE = "tickers_us.txt"
JP_TICKERS_FILE = "tickers_jp.txt"

# ===== Dedup state =====
STATE_FILE = "state.json"  # 같은 신호 반복 알림 방지용 (워크플로우가 자동 커밋)

# ===== Telegram =====
def tg_send(chat_id: str, text: str):
    if not TG_BOT_TOKEN:
        raise RuntimeError("TG_BOT_TOKEN이 비어있습니다 (GitHub Secrets 설정 필요).")
    if not chat_id:
        raise RuntimeError("채널 chat_id가 비어있습니다 (예: @us_ai_radar).")

    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True,
    }

    last_err = None
    for i in range(3):
        try:
            r = requests.post(url, data=payload, timeout=25)
            if r.status_code == 200:
                return
            if r.status_code == 429:
                time.sleep(2 + i * 2)
                continue
            last_err = f"Telegram API error {r.status_code}: {r.text[:300]}"
            break
        except requests.RequestException as e:
            last_err = f"Telegram request error: {repr(e)}"
            time.sleep(1 + i)

    raise RuntimeError(last_err or "Telegram send failed (unknown error)")

def safe_num(x):
    try:
        v = float(x)
        if math.isnan(v) or math.isinf(v):
            return None
        return v
    except Exception:
        return None

def load_tickers(path: str):
    if not os.path.exists(path):
        return []
    out = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            t = line.strip()
            if t and not t.startswith("#"):
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

def sig_key(market: str, ticker: str, last_date_key: str):
    # market + ticker + 마지막 캔들 날짜로 중복 방지
    return f"{market}|{ticker}|{INTERVAL}|{last_date_key}"

# ===== Core Scan =====
def scan_universe(tickers):
    hits = []
    for t in tickers:
        try:
            df = yf.download(t, period=PERIOD, interval=INTERVAL, progress=False)
            if df is None or len(df) < 30:
                continue

            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [c[0] for c in df.columns]

            if "Close" not in df.columns or "Volume" not in df.columns:
                continue

            df = df.dropna(subset=["Close", "Volume"])
            if len(df) < 30:
                continue

            df["MA20"] = df["Close"].rolling(20).mean()
            df["VOL20"] = df["Volume"].rolling(20).mean()

            last = df.iloc[-1]
            prev = df.iloc[-2]

            last_close = safe_num(last["Close"])
            prev_close = safe_num(prev["Close"])
            last_ma20 = safe_num(last["MA20"])
            prev_ma20 = safe_num(prev["MA20"])
            last_vol = safe_num(last["Volume"])
            last_vol20 = safe_num(last["VOL20"])

            if None in (last_close, prev_close, last_ma20, prev_ma20, last_vol, last_vol20):
                continue
            if last_ma20 == 0 or last_vol20 == 0:
                continue

            cross_up = (prev_close <= prev_ma20) and (last_close > last_ma20)
            vol_spike = last_vol >= (VOL_MULT * last_vol20)
            chg_pct = (last_close / prev_close - 1.0) * 100.0

            if cross_up and vol_spike and (chg_pct >= MIN_CHANGE_PCT):
                hits.append({
                    "ticker": t,
                    "chg_pct": chg_pct,
                    "close": last_close,
                    "vol_mult": last_vol / last_vol20,
                    "date_key": str(df.index[-1]),
                })

            time.sleep(0.12)
        except Exception:
            continue

    hits.sort(key=lambda x: x["chg_pct"], reverse=True)
    return hits

def format_hits(title, hits):
    now = datetime.now(KST).strftime("%Y-%m-%d %H:%M")
    lines = [f"📡 {title}", f"🕒 {now}", ""]
    if not hits:
        lines.append("- 조건 충족 종목: 없음")
        return "\n".join(lines)

    for h in hits[:25]:
        lines.append(
            f"- {h['ticker']} | {h['chg_pct']:+.2f}% | 종가 {h['close']:.2f} | 거래량 {h['vol_mult']:.1f}x"
        )
    return "\n".join(lines)

def filter_dedup(market: str, hits, state):
    sent = state.setdefault("sent", {})
    out = []
    for h in hits:
        k = sig_key(market, h["ticker"], h["date_key"])
        if sent.get(k):
            continue
        sent[k] = True
        out.append(h)
    return out

def main():
    now = datetime.now(KST).strftime("%Y-%m-%d %H:%M")

    us_tickers = load_tickers(US_TICKERS_FILE)
    jp_tickers = load_tickers(JP_TICKERS_FILE)

    state = load_state()

    # 1) 테스트 메시지 (원할 때만)
    if SEND_TEST == "1":
        if TG_CHAT_ID_US:
            tg_send(TG_CHAT_ID_US, f"✅ 업그레이드 레이더 테스트 (US) - {now}")
        if TG_CHAT_ID_JP:
            tg_send(TG_CHAT_ID_JP, f"✅ 업그레이드 레이더 테스트 (JP) - {now}")

    # 2) 미국
    if TG_CHAT_ID_US:
        us_hits = scan_universe(us_tickers) if us_tickers else []
        us_new = filter_dedup("US", us_hits, state)
        # 종목이 없으면 '없음'은 보내고, 종목이 있는데 전부 중복이면 조용히(스팸 방지)
        if not us_hits:
            tg_send(TG_CHAT_ID_US, format_hits(f"미국 20일선 돌파 + 거래량 {VOL_MULT:.1f}x", []))
        elif us_new:
            tg_send(TG_CHAT_ID_US, format_hits(f"미국 20일선 돌파 + 거래량 {VOL_MULT:.1f}x", us_new))

    # 3) 일본
    if TG_CHAT_ID_JP:
        jp_hits = scan_universe(jp_tickers) if jp_tickers else []
        jp_new = filter_dedup("JP", jp_hits, state)
        if not jp_hits:
            tg_send(TG_CHAT_ID_JP, format_hits(f"일본 20일선 돌파 + 거래량 {VOL_MULT:.1f}x", []))
        elif jp_new:
            tg_send(TG_CHAT_ID_JP, format_hits(f"일본 20일선 돌파 + 거래량 {VOL_MULT:.1f}x", jp_new))

    save_state(state)
    print("DONE")

if __name__ == "__main__":
    main()
