import os, time, requests
import pandas as pd
import smtplib
from email.mime.text import MIMEText

EOD_API_KEY = (os.getenv("EOD_API_KEY") or "").strip()

MAIL_TO_RAW = (os.getenv("HANMAIL_TO") or "").strip()
MAIL_FROM = (os.getenv("GMAIL_USER") or "").strip()
SMTP_USER = (os.getenv("GMAIL_USER") or "").strip()
SMTP_PASS = (os.getenv("GMAIL_APP_PASS") or "").strip()

SMTP_HOST = (os.getenv("SMTP_HOST") or "smtp.gmail.com").strip()
SMTP_PORT = int(os.getenv("SMTP_PORT") or "465")

TG_TOKEN = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
TG_CHAT_ID = (os.getenv("TELEGRAM_CHAT_ID") or "").strip()

TOPN = int(os.getenv("TOPN") or "30")
NEAR_PCT = float(os.getenv("NEAR_PCT") or "0.005")

US_LIST = ["AAPL","MSFT","NVDA","AMZN","META","GOOGL","TSLA","AMD","INTC","NFLX"]
JP_LIST = ["7203.T","6758.T","9984.T","8306.T","8035.T"]

def parse_recipients(raw: str):
    raw = (raw or "").strip().replace(";", ",")
    return [x.strip() for x in raw.split(",") if x.strip()]

def require_env():
    miss = []
    if not EOD_API_KEY: miss.append("EOD_API_KEY")
    if not parse_recipients(MAIL_TO_RAW): miss.append("HANMAIL_TO")
    if not SMTP_USER: miss.append("GMAIL_USER")
    if not SMTP_PASS: miss.append("GMAIL_APP_PASS")
    # 텔레그램은 있으면 같이 보냄(없으면 스킵)
    if miss:
        raise RuntimeError("Missing ENV: " + ", ".join(miss))

def tg_send(text: str):
    if not (TG_TOKEN and TG_CHAT_ID):
        print("[TG] token/chat_id missing -> skip", flush=True)
        return
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    # 텔레그램 메시지 제한 대응(대략 4000자 단위로 분할)
    chunks = []
    s = text
    while len(s) > 3900:
        chunks.append(s[:3900])
        s = s[3900:]
    chunks.append(s)

    for c in chunks:
        r = requests.post(url, data={"chat_id": TG_CHAT_ID, "text": c}, timeout=15)
        r.raise_for_status()

def fetch_eod(symbol):
    url = f"https://eodhistoricaldata.com/api/eod/{symbol}"
    params = {"api_token": EOD_API_KEY, "fmt": "json", "period": "d"}
    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    df = pd.DataFrame(r.json())
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values("date")

def scan(symbol, market):
    try:
        df = fetch_eod(symbol)
        if len(df) < 25:
            return None

        close = df["close"]
        ma20 = close.rolling(20).mean()

        c0, c1 = close.iloc[-1], close.iloc[-2]
        m0, m1 = ma20.iloc[-1], ma20.iloc[-2]

        breakout = (c1 < m1) and (c0 >= m0)
        near = abs(c0 / m0 - 1) <= NEAR_PCT

        if not (breakout or near):
            return None

        return {
            "symbol": symbol,
            "market": market,
            "close": round(float(c0), 2),
            "ma20": round(float(m0), 2),
            "pct": round((float(c0) / float(m0) - 1) * 100, 2),
            "type": "돌파" if breakout else "근접",
        }
    except Exception as e:
        print(f"[SKIP] {market}:{symbol} err={type(e).__name__}", flush=True)
        return None

def send_mail(rows):
    if not rows:
        print("[MAIL] no rows -> skip", flush=True)
        return

    to_list = parse_recipients(MAIL_TO_RAW)
    if not to_list:
        raise RuntimeError("HANMAIL_TO invalid")

    rows = sorted(rows, key=lambda x: (x["type"] != "돌파", -x["pct"]))

    subject = f"[미국·일본] MA20 돌파·근접 {len(rows)}종목"
    lines = [
        f"[{r['market']}] {r['symbol']} | {r['type']} | 종가 {r['close']} | MA20 {r['ma20']} | {r['pct']}%"
        for r in rows
    ]
    body = "\n".join(lines)

    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = MAIL_FROM
    msg["To"] = ", ".join(to_list)

    with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT) as s:
        s.login(SMTP_USER, SMTP_PASS)
        s.sendmail(MAIL_FROM, to_list, msg.as_string())

    print(f"[MAIL] sent ok -> {to_list}", flush=True)
    return subject, body

def main():
    require_env()
    print("[START] EOD MA20 US/JP scan", flush=True)

    results = []

    for i, s in enumerate(US_LIST, 1):
        print(f"[US] {i}/{len(US_LIST)} {s}", flush=True)
        r = scan(s, "US")
        if r: results.append(r)
        time.sleep(0.2)

    for i, s in enumerate(JP_LIST, 1):
        print(f"[JP] {i}/{len(JP_LIST)} {s}", flush=True)
        r = scan(s, "JP")
        if r: results.append(r)
        time.sleep(0.2)

    results = results[:TOPN]
    out = send_mail(results)

    # 텔레그램도 같이
    if out:
        subject, body = out
        tg_send(subject + "\n" + body)

if __name__ == "__main__":
    main()
