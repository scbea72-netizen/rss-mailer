import os
import io
import zipfile
import math
import time
import requests
import pandas as pd
from datetime import datetime, timezone, timedelta

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

KST = timezone(timedelta(hours=9))

# -----------------------------
# ENV
# -----------------------------
KIS_APPKEY = os.getenv("KIS_APPKEY", "").strip()
KIS_APPSECRET = os.getenv("KIS_APPSECRET", "").strip()
KIS_BASE_URL = os.getenv("KIS_BASE_URL", "https://openapi.koreainvestment.com:9443").strip()
KOSPI_URL = os.getenv("KIS_KOSPI_MST_URL", "").strip()
KOSDAQ_URL = os.getenv("KIS_KOSDAQ_MST_URL", "").strip()

SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com").strip()
SMTP_PORT = int(os.getenv("SMTP_PORT", "465"))
SMTP_USER = os.getenv("GMAIL_USER", "").strip()
SMTP_PASS = os.getenv("GMAIL_APP_PASS", "").strip()
MAIL_TO = os.getenv("HANMAIL_TO", "").strip()
MAIL_FROM = SMTP_USER

TOPN = int(os.getenv("TOPN", "30"))
VOL_MULT = float(os.getenv("VOL_MULT", "1.0"))
NEAR_PCT = float(os.getenv("NEAR_PCT", "0.005"))

TR_ID_CHART = "FHKST03010100"
TR_ID_PRICE = "FHKST01010100"

SLEEP_EVERY = 25
SLEEP_SEC = 0.25

# -----------------------------
# ETF 판별
# -----------------------------
def is_etf(name: str) -> bool:
    if not name:
        return False
    etf_keywords = [
        "KODEX", "TIGER", "KBSTAR", "ARIRANG",
        "HANARO", "KOSEF", "ACE", "SOL", "TIMEFOLIO"
    ]
    return any(k in name.upper() for k in etf_keywords)

# -----------------------------
# KIS API
# -----------------------------
def kis_token():
    r = requests.post(
        f"{KIS_BASE_URL}/oauth2/tokenP",
        json={
            "grant_type": "client_credentials",
            "appkey": KIS_APPKEY,
            "appsecret": KIS_APPSECRET
        },
        timeout=20
    )
    r.raise_for_status()
    return r.json()["access_token"]

def download_mst(url):
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.content

def load_mst(zip_bytes):
    zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
    name = [n for n in zf.namelist() if n.endswith(".mst")][0]
    text = zf.read(name).decode("cp949", errors="ignore")

    m = {}
    for line in text.splitlines():
        if len(line) < 10:
            continue
        code = line[:6]
        if code.isdigit():
            m[code] = line[6:40].strip()
    return m

def get_universe():
    kospi = load_mst(download_mst(KOSPI_URL))
    kosdaq = load_mst(download_mst(KOSDAQ_URL))
    name_map = {**kospi, **kosdaq}
    return sorted(name_map.keys()), name_map

def daily_chart(token, code):
    r = requests.get(
        f"{KIS_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
        headers={
            "authorization": f"Bearer {token}",
            "appkey": KIS_APPKEY,
            "appsecret": KIS_APPSECRET,
            "tr_id": TR_ID_CHART,
        },
        params={
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": code,
            "FID_PERIOD_DIV_CODE": "D",
            "FID_ORG_ADJ_PRC": "0",
        },
        timeout=20
    )
    r.raise_for_status()
    return r.json()

def industry_name(token, code):
    r = requests.get(
        f"{KIS_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-price",
        headers={
            "authorization": f"Bearer {token}",
            "appkey": KIS_APPKEY,
            "appsecret": KIS_APPSECRET,
            "tr_id": TR_ID_PRICE,
        },
        params={
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": code,
        },
        timeout=10
    )
    r.raise_for_status()
    return (r.json().get("output", {}).get("bstp_kor_isnm") or "기타").strip()

# -----------------------------
# Signal
# -----------------------------
def parse_df(j):
    rows = []
    for it in j.get("output2", []):
        try:
            rows.append({
                "date": pd.to_datetime(it["stck_bsop_date"]),
                "close": float(it["stck_clpr"]),
                "volume": float(it["acml_vol"])
            })
        except:
            pass
    df = pd.DataFrame(rows).sort_values("date")
    return df if len(df) >= 25 else None

def signal(df):
    c = df["close"]
    v = df["volume"]
    ma20 = c.rolling(20).mean()
    vma20 = v.rolling(20).mean()

    c0, c1 = c.iloc[-1], c.iloc[-2]
    m0, m1 = ma20.iloc[-1], ma20.iloc[-2]
    volx = v.iloc[-1] / vma20.iloc[-1]

    if volx < VOL_MULT:
        return None

    breakout = c1 < m1 and c0 >= m0
    near = abs(c0 / m0 - 1) <= NEAR_PCT

    if not (breakout or near):
        return None

    return {
        "close": c0,
        "ma20": m0,
        "pct": (c0 / m0 - 1) * 100,
        "volx": volx,
        "date": df["date"].iloc[-1].strftime("%Y-%m-%d"),
        "breakout": breakout,
        "near": near
    }

# -----------------------------
# Main
# -----------------------------
def main():
    token = kis_token()
    codes, name_map = get_universe()

    hits_b, hits_n = [], []
    industry_cache = {}

    for i, code in enumerate(codes, 1):
        try:
            df = parse_df(daily_chart(token, code))
            sig = signal(df)
            if not sig:
                continue

            if code not in industry_cache:
                industry_cache[code] = industry_name(token, code)

            base = {
                "code": code,
                "name": name_map.get(code, ""),
                "industry": industry_cache[code],
                "type": "ETF" if is_etf(name_map.get(code, "")) else "주식",
                **sig
            }

            if sig["breakout"]:
                hits_b.append(base)
            if sig["near"]:
                hits_n.append(base)

        except:
            pass

        if i % SLEEP_EVERY == 0:
            time.sleep(SLEEP_SEC)

    hits_b.sort(key=lambda x: abs(x["pct"]))
    hits_n = [x for x in hits_n if x["code"] not in {h["code"] for h in hits_b}]
    hits_n.sort(key=lambda x: abs(x["pct"]))

    hits_b = hits_b[:TOPN]
    hits_n = hits_n[:TOPN]

    # ---- 메일 ----
    def rows(items):
        return "\n".join(
            f"[{x['type']}] {x['code']} {x['name']} | {x['industry']} | "
            f"{x['pct']:+.2f}% | {x['volx']:.2f}x"
            for x in items
        )

    subject = f"[KIS] 20일선 돌파 {len(hits_b)} / 근접 {len(hits_n)}"
    body = f"""
[📈 돌파]
{rows(hits_b) or '없음'}

[👀 근접]
{rows(hits_n) or '없음'}
"""

    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = MAIL_FROM
    msg["To"] = MAIL_TO

    with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT) as s:
        s.login(SMTP_USER, SMTP_PASS)
        s.send_message(msg)

    print("OK")

if __name__ == "__main__":
    main()
