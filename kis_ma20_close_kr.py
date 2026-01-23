# === kis_ma20_close_kr.py (SPLIT SAFE VERSION) ===
# 전종목 FULL SCAN + GitHub Actions 분할 실행 대응 최종본

import os
import io
import zipfile
import time
import requests
import pandas as pd
from datetime import timezone, timedelta
import smtplib
from email.mime.text import MIMEText

# =========================
# SPLIT EXECUTION (핵심)
# =========================
PART_INDEX = int(os.getenv("PART_INDEX", "1"))
PART_TOTAL = int(os.getenv("PART_TOTAL", "1"))

def split_codes(codes):
    return [
        c for i, c in enumerate(codes)
        if i % PART_TOTAL == (PART_INDEX - 1)
    ]

# =========================
# ENV / CONST
# =========================
KST = timezone(timedelta(hours=9))

KIS_APPKEY = os.getenv("KIS_APPKEY", "").strip()
KIS_APPSECRET = os.getenv("KIS_APPSECRET", "").strip()
KIS_BASE_URL = os.getenv("KIS_BASE_URL", "https://openapi.koreainvestment.com:9443").strip()
KOSPI_URL = os.getenv("KIS_KOSPI_MST_URL", "").strip()
KOSDAQ_URL = os.getenv("KIS_KOSDAQ_MST_URL", "").strip()

SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com").strip()
SMTP_PORT = int(os.getenv("SMTP_PORT", "465"))
SMTP_USER = os.getenv("GMAIL_USER", "").strip()
SMTP_PASS = os.getenv("GMAIL_APP_PASS", "").strip()
MAIL_TO_RAW = (os.getenv("HANMAIL_TO") or "").strip()

TG_TOKEN = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
TG_CHAT_ID = (os.getenv("TELEGRAM_CHAT_ID") or "").strip()

TOPN = int(os.getenv("TOPN", "30"))
VOL_MULT = float(os.getenv("VOL_MULT", "0.7"))
NEAR_PCT = float(os.getenv("NEAR_PCT", "0.012"))
BREAKOUT_LOOKBACK = int(os.getenv("BREAKOUT_LOOKBACK", "3"))
ABOVE_MIN_PCT = float(os.getenv("ABOVE_MIN_PCT", "0.003"))
ABOVE_MAX_PCT = float(os.getenv("ABOVE_MAX_PCT", "0.05"))
ABOVE_VOL_MULT = float(os.getenv("ABOVE_VOL_MULT", "0.6"))

REQ_TIMEOUT = int(os.getenv("REQ_TIMEOUT", "15"))
SLEEP_EVERY = int(os.getenv("SLEEP_EVERY", "25"))
SLEEP_SEC = float(os.getenv("SLEEP_SEC", "0.35"))

TR_ID_CHART = "FHKST03010100"
TR_ID_PRICE = "FHKST01010100"

session = requests.Session()

# =========================
# VALIDATION
# =========================
def must_env():
    miss = []
    for k in ["KIS_APPKEY","KIS_APPSECRET","KIS_KOSPI_MST_URL","KIS_KOSDAQ_MST_URL",
              "GMAIL_USER","GMAIL_APP_PASS","HANMAIL_TO"]:
        if not os.getenv(k):
            miss.append(k)
    if miss:
        raise RuntimeError("Missing ENV: " + ", ".join(miss))

# =========================
# HTTP
# =========================
def req(method, url, **kw):
    r = session.request(method, url, timeout=REQ_TIMEOUT, **kw)
    r.raise_for_status()
    return r

# =========================
# KIS
# =========================
def kis_token():
    r = req("POST", f"{KIS_BASE_URL}/oauth2/tokenP",
            json={"grant_type":"client_credentials","appkey":KIS_APPKEY,"appsecret":KIS_APPSECRET})
    return r.json()["access_token"]

def load_mst(url):
    z = zipfile.ZipFile(io.BytesIO(req("GET", url).content))
    name = [n for n in z.namelist() if n.endswith(".mst")][0]
    txt = z.read(name).decode("cp949", errors="ignore")
    m = {}
    for line in txt.splitlines():
        if line[:6].isdigit():
            m[line[:6]] = line[6:40].strip()
    return m

def universe():
    kospi = load_mst(KOSPI_URL)
    kosdaq = load_mst(KOSDAQ_URL)
    name_map = {**kospi, **kosdaq}
    market = {**{c:"KOSPI" for c in kospi}, **{c:"KOSDAQ" for c in kosdaq}}
    return sorted(name_map.keys()), name_map, market

def chart(token, code):
    r = req("GET", f"{KIS_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
        headers={
            "authorization": f"Bearer {token}",
            "appkey": KIS_APPKEY,
            "appsecret": KIS_APPSECRET,
            "tr_id": TR_ID_CHART,
        },
        params={
            "FID_COND_MRKT_DIV_CODE":"J",
            "FID_INPUT_ISCD":code,
            "FID_PERIOD_DIV_CODE":"D",
            "FID_ORG_ADJ_PRC":"0",
        })
    return r.json()

# =========================
# SIGNAL
# =========================
def parse_df(j):
    rows=[]
    for it in j.get("output2",[]):
        rows.append({
            "date": pd.to_datetime(it["stck_bsop_date"]),
            "close": float(it["stck_clpr"]),
            "volume": float(it["acml_vol"])
        })
    if len(rows)<30: return None
    return pd.DataFrame(rows).sort_values("date")

def signal(df):
    c=df["close"]; v=df["volume"]
    ma20=c.rolling(20).mean(); vma=v.rolling(20).mean()
    if pd.isna(ma20.iloc[-1]): return None
    c0=float(c.iloc[-1]); m0=float(ma20.iloc[-1])
    volx=float(v.iloc[-1]/vma.iloc[-1])
    if volx<VOL_MULT: return None

    near=abs(c0/m0-1)<=NEAR_PCT
    breakout=False
    for i in range(len(df)-BREAKOUT_LOOKBACK-1,len(df)-1):
        if c.iloc[i]<ma20.iloc[i] and c.iloc[i+1]>=ma20.iloc[i+1]:
            breakout=True; break

    above=(c0>=m0 and ABOVE_MIN_PCT<=c0/m0-1<=ABOVE_MAX_PCT and volx>=ABOVE_VOL_MULT)

    if not (near or breakout or above): return None
    return {"pct":(c0/m0-1)*100,"volx":volx,"breakout":breakout,"near":near,"above":above}

# =========================
# MAIN
# =========================
def main():
    must_env()
    token=kis_token()

    codes, name_map, market = universe()
    my_codes = split_codes(codes)

    print(f"[SPLIT] part {PART_INDEX}/{PART_TOTAL} -> {len(my_codes)} / {len(codes)}", flush=True)

    hits=[]
    t0=time.time()

    for i,code in enumerate(my_codes,1):
        if i==1 or i%50==0:
            print(f"[PROGRESS][{PART_INDEX}] {i}/{len(my_codes)} elapsed={int(time.time()-t0)}s", flush=True)
        try:
            df=parse_df(chart(token,code))
            sig=signal(df)
            if sig:
                hits.append({
                    "code":code,
                    "name":name_map.get(code,""),
                    "market":market.get(code,""),
                    **sig
                })
        except Exception:
            pass
        if i%SLEEP_EVERY==0:
            time.sleep(SLEEP_SEC)

    os.makedirs("output",exist_ok=True)
    with open(f"output/result_part_{PART_INDEX}.json","w",encoding="utf-8") as f:
        f.write(pd.DataFrame(hits).to_json(orient="records",force_ascii=False))

    print(f"[DONE][{PART_INDEX}] hits={len(hits)}", flush=True)

if __name__=="__main__":
    main()
