import os
import io
import zipfile
import time
import requests
import pandas as pd
from datetime import timezone, timedelta

import smtplib
from email.mime.text import MIMEText

KST = timezone(timedelta(hours=9))

# -----------------------------
# ENV (유연 + 명확)
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

# 수신자: HANMAIL_TO 기준 (여러 주소 허용)
MAIL_TO_RAW = (os.getenv("HANMAIL_TO") or "").strip()
MAIL_FROM = SMTP_USER

TOPN = int(os.getenv("TOPN", "30"))
VOL_MULT = float(os.getenv("VOL_MULT", "1.0"))
NEAR_PCT = float(os.getenv("NEAR_PCT", "0.005"))

# 안전 옵션
TEST_LIMIT = int(os.getenv("TEST_LIMIT", "400"))
FULL_SCAN = os.getenv("FULL_SCAN", "0").strip() == "1"

REQ_TIMEOUT = int(os.getenv("REQ_TIMEOUT", "15"))
MST_TIMEOUT = int(os.getenv("MST_TIMEOUT", "60"))
MAX_RETRY = int(os.getenv("MAX_RETRY", "2"))
RETRY_SLEEP = float(os.getenv("RETRY_SLEEP", "0.6"))

SLEEP_EVERY = int(os.getenv("SLEEP_EVERY", "25"))
SLEEP_SEC = float(os.getenv("SLEEP_SEC", "0.35"))

TR_ID_CHART = "FHKST03010100"
TR_ID_PRICE = "FHKST01010100"

session = requests.Session()

def parse_recipients(raw: str):
    raw = (raw or "").strip().replace(";", ",")
    return [x.strip() for x in raw.split(",") if x.strip()]

def must_env():
    miss = []
    if not KIS_APPKEY: miss.append("KIS_APPKEY")
    if not KIS_APPSECRET: miss.append("KIS_APPSECRET")
    if not KOSPI_URL: miss.append("KIS_KOSPI_MST_URL")
    if not KOSDAQ_URL: miss.append("KIS_KOSDAQ_MST_URL")
    if not SMTP_USER: miss.append("GMAIL_USER")
    if not SMTP_PASS: miss.append("GMAIL_APP_PASS")
    if not parse_recipients(MAIL_TO_RAW): miss.append("HANMAIL_TO")
    if miss:
        raise RuntimeError("Missing ENV: " + ", ".join(miss))

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
    up = name.upper()
    return any(k in up for k in etf_keywords)

# -----------------------------
# HTTP helper
# -----------------------------
def request_with_retry(method, url, *, headers=None, params=None, json=None, timeout=REQ_TIMEOUT):
    last_err = None
    for attempt in range(1, MAX_RETRY + 2):
        try:
            r = session.request(
                method, url,
                headers=headers, params=params, json=json,
                timeout=timeout
            )
            r.raise_for_status()
            return r
        except Exception as e:
            last_err = e
            time.sleep(RETRY_SLEEP)
    raise last_err

# -----------------------------
# KIS API
# -----------------------------
def kis_token():
    r = request_with_retry(
        "POST",
        f"{KIS_BASE_URL}/oauth2/tokenP",
        json={
            "grant_type": "client_credentials",
            "appkey": KIS_APPKEY,
            "appsecret": KIS_APPSECRET
        },
        timeout=REQ_TIMEOUT
    )
    return r.json()["access_token"]

def download_mst(url):
    r = request_with_retry("GET", url, timeout=MST_TIMEOUT)
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
    codes = sorted(name_map.keys())
    return codes, name_map

def daily_chart(token, code):
    r = request_with_retry(
        "GET",
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
        timeout=REQ_TIMEOUT
    )
    return r.json()

def industry_name(token, code):
    r = request_with_retry(
        "GET",
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
        timeout=REQ_TIMEOUT
    )
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
        except Exception:
            pass
    if not rows:
        return None
    df = pd.DataFrame(rows).sort_values("date")
    return df if len(df) >= 25 else None

def signal(df):
    if df is None or len(df) < 25:
        return None

    c = df["close"]
    v = df["volume"]
    ma20 = c.rolling(20).mean()
    vma20 = v.rolling(20).mean()

    if pd.isna(ma20.iloc[-1]) or pd.isna(vma20.iloc[-1]) or vma20.iloc[-1] == 0:
        return None

    c0, c1 = c.iloc[-1], c.iloc[-2]
    m0, m1 = ma20.iloc[-1], ma20.iloc[-2]

    volx = v.iloc[-1] / vma20.iloc[-1]
    if volx < VOL_MULT:
        return None

    breakout = (c1 < m1) and (c0 >= m0)
    near = abs(c0 / m0 - 1) <= NEAR_PCT
    if not (breakout or near):
        return None

    return {
        "close": float(c0),
        "ma20": float(m0),
        "pct": float((c0 / m0 - 1) * 100),
        "volx": float(volx),
        "date": df["date"].iloc[-1].strftime("%Y-%m-%d"),
        "breakout": breakout,
        "near": near
    }

# -----------------------------
# Mail (핵심 수정)
# -----------------------------
def send_mail(hits_b, hits_n):
    to_list = parse_recipients(MAIL_TO_RAW)
    if not to_list:
        raise RuntimeError("HANMAIL_TO is empty or invalid")

    def fmt_rows(items):
        return "\n".join(
            f"[{x['type']}] {x['code']} {x['name']} | {x['industry']} | "
            f"{x['pct']:+.2f}% | {x['volx']:.2f}x"
            for x in items
        )

    subject = f"[KIS] 20일선 돌파 {len(hits_b)} / 근접 {len(hits_n)}"
    body = f"""\
[📈 돌파]
{fmt_rows(hits_b) or '없음'}

[👀 근접]
{fmt_rows(hits_n) or '없음'}
"""

    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = MAIL_FROM
    msg["To"] = ", ".join(to_list)

    with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT) as s:
        s.login(SMTP_USER, SMTP_PASS)
        # ✅ send_message() 대신 sendmail()
        s.sendmail(MAIL_FROM, to_list, msg.as_string())

    print(f"[MAIL] sent ok -> {to_list}", flush=True)

# -----------------------------
# Main
# -----------------------------
def main():
    must_env()

    print("[START] get token", flush=True)
    token = kis_token()

    print("[START] load universe", flush=True)
    codes, name_map = get_universe()

    if not FULL_SCAN:
        codes = codes[:TEST_LIMIT]
        print(f"[MODE] TEST_LIMIT={TEST_LIMIT} (FULL_SCAN=0)", flush=True)
    else:
        print(f"[MODE] FULL_SCAN=1 (TOTAL={len(codes)})", flush=True)

    hits_b, hits_n = [], []
    industry_cache = {}

    total = len(codes)
    t0 = time.time()

    for i, code in enumerate(codes, 1):
        if i == 1 or i % 50 == 0:
            elapsed = int(time.time() - t0)
            print(f"[PROGRESS] {i}/{total} elapsed={elapsed}s last={code}", flush=True)

        try:
            j = daily_chart(token, code)
            df = parse_df(j)
            sig = signal(df)
            if not sig:
                continue

            if code not in industry_cache:
                try:
                    industry_cache[code] = industry_name(token, code)
                except Exception:
                    industry_cache[code] = "기타"

            nm = name_map.get(code, "")
            base = {
                "code": code,
                "name": nm,
                "industry": industry_cache[code],
                "type": "ETF" if is_etf(nm) else "주식",
                **sig
            }

            if sig["breakout"]:
                hits_b.append(base)
            if sig["near"]:
                hits_n.append(base)

        except Exception as e:
            print(f"[SKIP] {code} err={type(e).__name__}", flush=True)

        if i % SLEEP_EVERY == 0:
            time.sleep(SLEEP_SEC)

    hits_b.sort(key=lambda x: x["pct"], reverse=True)
    bset = {h["code"] for h in hits_b}
    hits_n = [x for x in hits_n if x["code"] not in bset]
    hits_n.sort(key=lambda x: abs(x["pct"]))

    hits_b = hits_b[:TOPN]
    hits_n = hits_n[:TOPN]

    print(f"[RESULT] breakout={len(hits_b)} near={len(hits_n)}", flush=True)

    send_mail(hits_b, hits_n)
    print("[OK] mail sent", flush=True)

if __name__ == "__main__":
    main()

