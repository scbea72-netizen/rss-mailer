# 통째로 수정본: US / JP MA20 돌파·근접 스캐너 (장마감용)
# - SMTPRecipientsRefused(555 5.5.2) 해결: 수신자 파싱 + sendmail 사용
# - 안전장치: ENV 체크, 진행 로그(flush), timeout 유지

import os, time, requests
import pandas as pd
from datetime import timezone, timedelta
import smtplib
from email.mime.text import MIMEText

# ================== 환경 변수 ==================
EOD_API_KEY = (os.getenv("EOD_API_KEY") or "").strip()
MAIL_TO_RAW = (os.getenv("HANMAIL_TO") or "").strip()   # 콤마/세미콜론 가능
MAIL_FROM = (os.getenv("GMAIL_USER") or "").strip()
SMTP_USER = (os.getenv("GMAIL_USER") or "").strip()
SMTP_PASS = (os.getenv("GMAIL_APP_PASS") or "").strip()

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 465

TOPN = 30
NEAR_PCT = 0.005  # ±0.5%
KST = timezone(timedelta(hours=9))

# ================== 유니버스 ==================
US_LIST = [
    "AAPL","MSFT","NVDA","AMZN","META","GOOGL","TSLA","AMD","INTC","NFLX",
    "AVGO","QCOM","MU","ADBE","ORCL","CRM","NOW","ASML","ARM","SMCI"
]
JP_LIST = [
    "7203.T","6758.T","9984.T","8306.T","8035.T",
    "4063.T","6861.T","9432.T","4502.T","6501.T"
]

# ================== 공통 유틸 ==================
def require_env():
    miss = []
    if not EOD_API_KEY: miss.append("EOD_API_KEY")
    if not MAIL_TO_RAW: miss.append("HANMAIL_TO")
    if not SMTP_USER: miss.append("GMAIL_USER")
    if not SMTP_PASS: miss.append("GMAIL_APP_PASS")
    if miss:
        raise RuntimeError("Missing ENV: " + ", ".join(miss))

def parse_recipients(raw: str):
    # "a@x.com, b@y.com" / "a@x.com;b@y.com" 모두 지원
    raw = (raw or "").strip()
    raw = raw.replace(";", ",")
    to_list = [x.strip() for x in raw.split(",") if x.strip()]
    return to_list

# ================== 데이터 수집 ==================
def fetch_eod(symbol: str) -> pd.DataFrame:
    url = f"https://eodhistoricaldata.com/api/eod/{symbol}"
    params = {
        "api_token": EOD_API_KEY,
        "fmt": "json",
        "period": "d"
    }
    # ✅ timeout 유지 (무한 대기 방지)
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
            "close": round(float(c0), 2),
            "ma20": round(float(m0), 2),
            "pct": round((float(c0) / float(m0) - 1) * 100, 2),
            "type": "돌파" if breakout else "근접"
        }
    except Exception as e:
        # 로그 남기고 스킵
        print(f"[SKIP] {market}:{symbol} err={type(e).__name__}", flush=True)
        return None

# ================== 메일 발송 ==================
def send_mail(rows):
    if not rows:
        print("[MAIL] no rows -> skip", flush=True)
        return

    to_list = parse_recipients(MAIL_TO_RAW)
    if not to_list:
        # 여기서부터 SMTPRecipientsRefused(수신자 '') 같은 오류가 났던 케이스 방지
        raise RuntimeError("HANMAIL_TO is empty/invalid (no recipients parsed)")

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
    msg["To"] = ", ".join(to_list)

    with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT) as s:
        s.login(SMTP_USER, SMTP_PASS)
        # ✅ 핵심 수정: send_message() 대신 sendmail()로 수신자 리스트를 명시 전달
        s.sendmail(MAIL_FROM, to_list, msg.as_string())

    print(f"[MAIL] sent ok -> {to_list}", flush=True)

# ================== 메인 ==================
def main():
    require_env()

    results = []

    for i, s in enumerate(US_LIST, 1):
        print(f"[US] {i}/{len(US_LIST)} {s}", flush=True)
        r = scan(s, "US")
        if r:
            results.append(r)
        time.sleep(0.25)

    for i, s in enumerate(JP_LIST, 1):
        print(f"[JP] {i}/{len(JP_LIST)} {s}", flush=True)
        r = scan(s, "JP")
        if r:
            results.append(r)
        time.sleep(0.25)

    results = results[:TOPN]
    send_mail(results)

if __name__ == "__main__":
    main()
