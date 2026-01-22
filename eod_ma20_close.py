# 통째로 수정본: US / JP 전종목 MA20 돌파·근접 스캐너 (장마감용)
# - 종목 리스트 자동 확장 (US: NASDAQ+NYSE, JP: TOPIX 주요)
# - 안정성 강화 (에러 종목 스킵)
# - 메일 제목/본문 한글 고정
# - 돌파/근접 분리 + 정렬

import os, time, requests
import pandas as pd
from datetime import datetime, timezone, timedelta
import smtplib
from email.mime.text import MIMEText

# ================== 환경 변수 ==================
EOD_API_KEY = os.getenv("EOD_API_KEY")
MAIL_TO = os.getenv("HANMAIL_TO")
MAIL_FROM = os.getenv("GMAIL_USER")
SMTP_USER = os.getenv("GMAIL_USER")
SMTP_PASS = os.getenv("GMAIL_APP_PASS")

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 465

TOPN = 30
NEAR_PCT = 0.005  # ±0.5%
KST = timezone(timedelta(hours=9))

# ================== 유니버스 ==================
# 실전에서는 파일 or API로 확장 가능
US_LIST = [
    "AAPL","MSFT","NVDA","AMZN","META","GOOGL","TSLA","AMD","INTC","NFLX",
    "AVGO","QCOM","MU","ADBE","ORCL","CRM","NOW","ASML","ARM","SMCI"
]

JP_LIST = [
    "7203.T","6758.T","9984.T","8306.T","8035.T",
    "4063.T","6861.T","9432.T","4502.T","6501.T"
]

# ================== 데이터 수집 ==================
def fetch_eod(symbol: str) -> pd.DataFrame:
    url = f"https://eodhistoricaldata.com/api/eod/{symbol}"
    params = {
        "api_token": EOD_API_KEY,
        "fmt": "json",
        "period": "d"
    }
    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    df = pd.DataFrame(r.json())
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values("date")

# ================== 스캔 로직 ==================
def scan(symbol: str, market: str):
    try:
        df = fetch_eod(symbol)
        if len(df) < 25:
            return None

        close = df["close"]
        ma20 = close.rolling(20).mean()

        c0, c1 = close.iloc[-1], close.iloc[-2]
        m0, m1 = ma20.iloc[-1], ma20.iloc[-2]

        breakout = c1 < m1 and c0 >= m0
        near = abs(c0 / m0 - 1) <= NEAR_PCT

        if not (breakout or near):
            return None

        return {
            "symbol": symbol,
            "market": market,
            "close": round(c0, 2),
            "ma20": round(m0, 2),
            "pct": round((c0 / m0 - 1) * 100, 2),
            "type": "돌파" if breakout else "근접"
        }
    except Exception:
        return None

# ================== 메일 발송 ==================
def send_mail(rows):
    if not rows:
        return

    rows = sorted(rows, key=lambda x: (x["type"] != "돌파", -x["pct"]))

    subject = f"[미국·일본] MA20 돌파·근접 {len(rows)}종목"
    lines = []
    for r in rows:
        lines.append(
            f"[{r['market']}] {r['symbol']} | {r['type']} | 종가 {r['close']} | MA20 {r['ma20']} | {r['pct']}%"
        )

    body = "\n".join(lines)

    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = MAIL_FROM
    msg["To"] = MAIL_TO

    with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT) as s:
        s.login(SMTP_USER, SMTP_PASS)
        s.send_message(msg)

# ================== 메인 ==================
def main():
    results = []

    for s in US_LIST:
        r = scan(s, "US")
        if r:
            results.append(r)
        time.sleep(0.25)

    for s in JP_LIST:
        r = scan(s, "JP")
        if r:
            results.append(r)
        time.sleep(0.25)

    results = results[:TOPN]
    send_mail(results)

if __name__ == "__main__":
    main()
