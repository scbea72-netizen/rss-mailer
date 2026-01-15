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

# 채널(또는 그룹) 대상: 공개채널이면 @채널아이디 사용 권장
# 예) TG_CHAT_ID_US="@us_ai_radar"
#     TG_CHAT_ID_JP="@jp_ai_radar"
TG_CHAT_ID_US = os.getenv("TG_CHAT_ID_US", "").strip()
TG_CHAT_ID_JP = os.getenv("TG_CHAT_ID_JP", "").strip()

# ===== ENV (옵션: 기준 튜닝) =====
# 거래량 폭증 배수 (기본 2.0배)
VOL_MULT = float(os.getenv("VOL_MULT", "2.0"))
# 전일대비 상승률 최소(%) (기본 0% = 조건 없음)
MIN_CHANGE_PCT = float(os.getenv("MIN_CHANGE_PCT", "0"))
# 캔들 간격/기간 (기본 1d / 6mo)
INTERVAL = os.getenv("INTERVAL", "1d")
PERIOD = os.getenv("PERIOD", "6mo")

# ===== Universe (원하면 자유롭게 추가/삭제) =====
US_TICKERS = [
    # AI/반도체/빅테크 중심 예시
    "NVDA", "AMD", "INTC", "TSM", "ASML",
    "MSFT", "AMZN", "GOOGL", "META", "AAPL",
    "AVGO", "MU", "QCOM", "AMAT", "LRCX"
]

JP_TICKERS = [
    # 일본 대표 예시 (원하면 추가)
    "7203.T",  # Toyota
    "6758.T",  # Sony
    "9984.T",  # SoftBank Group
    "8035.T",  # Tokyo Electron
    "6857.T",  # Advantest
    "9432.T",  # NTT
    "6861.T",  # Keyence (예시)
]

# ===== Telegram =====
def tg_send(chat_id: str, text: str):
    if not TG_BOT_TOKEN:
        raise RuntimeError("TG_BOT_TOKEN이 비어있습니다 (GitHub Secrets 설정 필요).")
    if not chat_id:
        # 채널 미설정이면 조용히 skip
        return

    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True,
    }

    # 간단 재시도(네트워크 순간 오류 대비)
    for i in range(3):
        try:
            r = requests.post(url, data=payload, timeout=20)
            if r.status_code == 200:
                return
            # 429(Too Many Requests)면 조금 쉬었다 재시도
            if r.status_code == 429:
                time.sleep(2 + i * 2)
                continue
            # 그 외는 에러 로그
            raise RuntimeError(f"Telegram API error {r.status_code}: {r.text[:200]}")
        except requests.RequestException as e:
            if i == 2:
                raise
            time.sleep(1 + i)

def safe_num(x):
    try:
        v = float(x)
        if math.isnan(v) or math.isinf(v):
            return None
        return v
    except Exception:
        return None

# ===== Core Scan =====
def scan_universe(tickers, label):
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

            # 멀티인덱스 방지(간혹 yfinance가 컬럼 구조 다르게 주는 경우 대비)
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [c[0] for c in df.columns]

            # 필요한 컬럼 체크
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

            # 너무 빠르게 호출하면 가끔 막힐 수 있어 약간 쉼
            time.sleep(0.2)

        except Exception:
            # 한 종목 실패해도 전체는 계속
            continue

    # 변동률 큰 순 정렬
    hits.sort(key=lambda x: x["chg_pct"], reverse=True)
    return hits

def format_message(title, hits):
    now = datetime.now(KST).strftime("%Y-%m-%d %H:%M")
    if not hits:
        return ""

    lines = [f"🚨 {title}", f"🕒 {now}", ""]
    for h in hits:
        # 가격 소수점: 미국은 보통 소수, 일본은 엔 단위지만 그냥 2자리로 통일
        lines.append(
            f"- {h['ticker']} | +{h['chg_pct']:.2f}% | 종가 {h['close']:.2f} | 거래량 {h['vol_mult']:.1f}x"
        )
    return "\n".join(lines)

def main():
    # 미국
    us_hits = scan_universe(US_TICKERS, "US")
    us_msg = format_message("미국 20일선 돌파 + 거래량 폭증", us_hits)
    if us_msg:
        tg_send(TG_CHAT_ID_US, us_msg)

    # 일본
    jp_hits = scan_universe(JP_TICKERS, "JP")
    jp_msg = format_message("일본 20일선 돌파 + 거래량 폭증", jp_hits)
    if jp_msg:
        tg_send(TG_CHAT_ID_JP, jp_msg)

    # Actions 로그용
    print("US hits:", len(us_hits), "JP hits:", len(jp_hits))

if __name__ == "__main__":
    main()

