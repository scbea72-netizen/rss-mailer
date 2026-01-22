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
        if len(
