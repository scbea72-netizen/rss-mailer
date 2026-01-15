import os
import time
import math
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
VOL_MULT = float(os.getenv("VOL_MULT", "2.0"))          # 거래량 폭증 배수
MIN_CHANGE_PCT = float(os.getenv("MIN_CHANGE_PCT", "0"))# 전일대비 상승률 최소(%)
INTERVAL = os.getenv("INTERVAL", "1d")
PERIOD = os.getenv("PERIOD", "6mo")

# ✅ 무조건 테스트 메시지 보낼지 (기본 ON)
SEND_TEST = os.getenv("SEND_TEST", "1").strip()         # "1"=보냄, "0"=안보냄

# ===== Universe =====
US_TICKERS = [
    "NVDA", "AMD", "INTC", "TSM", "ASML",
    "MSFT", "AMZN", "GOOGL", "META", "AAPL",
    "AVGO", "MU", "QCOM", "AMAT", "LRCX"
]

JP_TICKERS = [
    "7203.T",  # Toyota
    "6758.T",  # Sony
    "9984.T",  # SoftBank Group
    "8035.T",  # Tokyo Electron
    "6857.T",  # Advantest
    "9432.T",  # NTT
    "6861.T",  # Keyence
]

# ===== Telegram =====
def tg_send(chat_id: str, text: str):
    """
    실패 시 GitHub Actions 로그에 이유가 뜨도록 예외를 올립니다.
    """
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

    # 간단 재시도
    last_err = None
    for i in range(3):
        try:
            r = requests.post(url, data=payload, timeout=25)
            if r.status_code == 200:
                return
            # 429면 대기 후 재시도
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

# ===== Core Scan =====
def scan_universe(tickers):
    """
    조건:
    - (전일 종가 <= 전일 MA20) AND (금일 종가 > 금일 MA20) : 20일선 상향돌파
    - 금일 거래량 >= VOL_MULT * 금일 VOL20
    - 금일 변동률 >= MIN_CHANGE_PCT
    """
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
                })

            time.sleep(0.15)

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

def main():
    now = datetime.now(KST).strftime("%Y-%m-%d %H:%M")

    # ✅ 1) 먼저 테스트 메시지(연결 확인용) — 기본 ON
    if SEND_TEST == "1":
        if TG_CHAT_ID_US:
            tg_send(TG_CHAT_ID_US, f"✅ 레이더 테스트 발송 성공 (US) - {now}")
        if TG_CHAT_ID_JP:
            tg_send(TG_CHAT_ID_JP, f"✅ 레이더 테스트 발송 성공 (JP) - {now}")

    # ✅ 2) 종목 결과는 '없음'이어도 항상 메시지 발송
    if TG_CHAT_ID_US:
        us_hits = scan_universe(US_TICKERS)
        tg_send(TG_CHAT_ID_US, format_hits(f"미국 20일선 돌파 + 거래량 {VOL_MULT:.1f}x", us_hits))

    if TG_CHAT_ID_JP:
        jp_hits = scan_universe(JP_TICKERS)
        tg_send(TG_CHAT_ID_JP, format_hits(f"일본 20일선 돌파 + 거래량 {VOL_MULT:.1f}x", jp_hits))

    print("DONE")

if __name__ == "__main__":
    main()
