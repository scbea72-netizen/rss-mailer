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
MAIL_FROM = SMTP_USER

TG_TOKEN = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
TG_CHAT_ID = (os.getenv("TELEGRAM_CHAT_ID") or "").strip()

TOPN = int(os.getenv("TOPN", "30"))

# ✅ 현실형 기본값
VOL_MULT = float(os.getenv("VOL_MULT", "0.7"))          # 평균 거래량의 70%만 넘어도 통과
NEAR_PCT = float(os.getenv("NEAR_PCT", "0.012"))        # ±1.2%

# ✅ 최근 며칠 내 돌파도 포함
BREAKOUT_LOOKBACK = int(os.getenv("BREAKOUT_LOOKBACK", "3"))

# ✅ 유지(상방 유지) 트랙
ABOVE_MIN_PCT = float(os.getenv("ABOVE_MIN_PCT", "0.003"))  # +0.3% 이상
ABOVE_MAX_PCT = float(os.getenv("ABOVE_MAX_PCT", "0.05"))   # +5% 이내
ABOVE_VOL_MULT = float(os.getenv("ABOVE_VOL_MULT", "0.6"))  # 평균의 60%만 넘어도

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


def request_with_retry(method, url, *, headers=None, params=None, data=None, json=None, timeout=REQ_TIMEOUT):
    last_err = None
    for _ in range(1, MAX_RETRY + 2):
        try:
            r = session.request(method, url, headers=headers, params=params, data=data, json=json, timeout=timeout)
            r.raise_for_status()
            return r
        except Exception as e:
            last_err = e
            time.sleep(RETRY_SLEEP)
    raise last_err


# -----------------------------
# Telegram helpers
# -----------------------------
def tg_debug_env():
    print(f"[TG] token_len={len(TG_TOKEN)} chat_id_len={len(TG_CHAT_ID)}", flush=True)


def tg_api_base():
    return f"https://api.telegram.org/bot{TG_TOKEN}"


def tg_check_token():
    if not TG_TOKEN:
        return False, "token missing"
    try:
        r = request_with_retry("GET", f"{tg_api_base()}/getMe", timeout=15)
        j = r.json()
        if j.get("ok"):
            return True, "ok"
        return False, f"getMe not ok: {str(j)[:200]}"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def tg_send(text: str):
    if not (TG_TOKEN and TG_CHAT_ID):
        print("[TG] token/chat_id missing -> skip", flush=True)
        return False

    ok, msg = tg_check_token()
    if not ok:
        print(f"[TG] token invalid -> skip. reason={msg}", flush=True)
        return False

    url = f"{tg_api_base()}/sendMessage"

    chunks = []
    s = text or ""
    while len(s) > 3900:
        chunks.append(s[:3900])
        s = s[3900:]
    chunks.append(s)

    sent = 0
    for c in chunks:
        try:
            request_with_retry("POST", url, data={"chat_id": TG_CHAT_ID, "text": c}, timeout=15)
            sent += 1
        except Exception as e:
            print(f"[TG] send failed chunk={sent+1}/{len(chunks)} err={type(e).__name__}: {e}", flush=True)
            return False

    print(f"[TG] sent ok chunks={sent}", flush=True)
    return True


# -----------------------------
# KIS helpers
# -----------------------------
def kis_token():
    r = request_with_retry(
        "POST",
        f"{KIS_BASE_URL}/oauth2/tokenP",
        json={"grant_type": "client_credentials", "appkey": KIS_APPKEY, "appsecret": KIS_APPSECRET},
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


def get_universe_with_market():
    """
    ✅ 코스피/코스닥 구분용: code -> market 맵 구성
    """
    kospi = load_mst(download_mst(KOSPI_URL))
    kosdaq = load_mst(download_mst(KOSDAQ_URL))

    name_map = {**kospi, **kosdaq}
    market_map = {}
    for c in kospi.keys():
        market_map[c] = "KOSPI"
    for c in kosdaq.keys():
        market_map[c] = "KOSDAQ"

    codes = sorted(name_map.keys())
    return codes, name_map, market_map


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


def is_etf(name: str) -> bool:
    if not name:
        return False
    etf_keywords = ["KODEX", "TIGER", "KBSTAR", "ARIRANG", "HANARO", "KOSEF", "ACE", "SOL", "TIMEFOLIO"]
    up = name.upper()
    return any(k in up for k in etf_keywords)


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
    return df if len(df) >= 30 else None


def signal(df):
    if df is None or len(df) < 30:
        return None

    c = df["close"].astype(float)
    v = df["volume"].astype(float)
    ma20 = c.rolling(20).mean()
    vma20 = v.rolling(20).mean()

    if pd.isna(ma20.iloc[-1]) or pd.isna(vma20.iloc[-1]) or vma20.iloc[-1] == 0:
        return None

    c0 = float(c.iloc[-1])
    m0 = float(ma20.iloc[-1])
    pct = (c0 / m0 - 1.0) * 100.0
    volx = float(v.iloc[-1] / vma20.iloc[-1])

    # 거래량(현실형) 필터
    if volx < VOL_MULT:
        return None

    # 근접
    near = abs(c0 / m0 - 1.0) <= NEAR_PCT

    # 돌파(최근 lookback일)
    lb = max(1, int(BREAKOUT_LOOKBACK))
    breakout = False
    start = max(1, len(df) - lb - 1)
    for i in range(start, len(df) - 1):
        if pd.isna(ma20.iloc[i]) or pd.isna(ma20.iloc[i + 1]):
            continue
        if c.iloc[i] < ma20.iloc[i] and c.iloc[i + 1] >= ma20.iloc[i + 1]:
            breakout = True
            break

    # 유지(상방 유지)
    above = False
    if c0 >= m0:
        dist = (c0 / m0 - 1.0)
        if (dist >= ABOVE_MIN_PCT) and (dist <= ABOVE_MAX_PCT) and (volx >= ABOVE_VOL_MULT):
            above = True

    if not (breakout or near or above):
        return None

    return {
        "close": c0,
        "ma20": m0,
        "pct": float(pct),
        "volx": float(volx),
        "date": df["date"].iloc[-1].strftime("%Y-%m-%d"),
        "breakout": breakout,
        "near": near,
        "above": above,
    }


def split_by_market(items):
    kospi = [x for x in items if x.get("market") == "KOSPI"]
    kosdaq = [x for x in items if x.get("market") == "KOSDAQ"]
    return kospi, kosdaq


def sort_and_dedupe(hits_b, hits_n, hits_a):
    # 우선순위: 돌파 > 근접 > 유지
    hits_b.sort(key=lambda x: x["pct"], reverse=True)
    bset = {h["code"] for h in hits_b}

    hits_n = [x for x in hits_n if x["code"] not in bset]
    hits_n.sort(key=lambda x: abs(x["pct"]))
    bnset = bset | {h["code"] for h in hits_n}

    hits_a = [x for x in hits_a if x["code"] not in bnset]
    hits_a.sort(key=lambda x: x["pct"], reverse=True)

    return hits_b[:TOPN], hits_n[:TOPN], hits_a[:TOPN]


def fmt_rows(items):
    return "\n".join(
        f"[{x['type']}] {x['code']} {x['name']} | {x['industry']} | {x['pct']:+.2f}% | {x['volx']:.2f}x"
        for x in items
    )


def send_mail(kospi_b, kospi_n, kospi_a, kosdaq_b, kosdaq_n, kosdaq_a):
    to_list = parse_recipients(MAIL_TO_RAW)
    if not to_list:
        raise RuntimeError("HANMAIL_TO invalid")

    subject = (
        f"[KIS] "
        f"KOSPI 돌파{len(kospi_b)}/근접{len(kospi_n)}/유지{len(kospi_a)} | "
        f"KOSDAQ 돌파{len(kosdaq_b)}/근접{len(kosdaq_n)}/유지{len(kosdaq_a)}"
    )

    body = f"""[KOSPI 📌]
[📈 돌파(최근 {BREAKOUT_LOOKBACK}일)]
{fmt_rows(kospi_b) or '없음'}

[👀 근접(±{NEAR_PCT*100:.1f}%)]
{fmt_rows(kospi_n) or '없음'}

[✅ 유지(상방 유지)]
{fmt_rows(kospi_a) or '없음'}


[KOSDAQ 📌]
[📈 돌파(최근 {BREAKOUT_LOOKBACK}일)]
{fmt_rows(kosdaq_b) or '없음'}

[👀 근접(±{NEAR_PCT*100:.1f}%)]
{fmt_rows(kosdaq_n) or '없음'}

[✅ 유지(상방 유지)]
{fmt_rows(kosdaq_a) or '없음'}
"""

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
    must_env()
    tg_debug_env()

    print("[START] get token", flush=True)
    token = kis_token()

    print("[START] load universe", flush=True)
    codes, name_map, market_map = get_universe_with_market()

    if not FULL_SCAN:
        codes = codes[:TEST_LIMIT]
        print(f"[MODE] TEST_LIMIT={TEST_LIMIT} (FULL_SCAN=0)", flush=True)
    else:
        print(f"[MODE] FULL_SCAN=1 (TOTAL={len(codes)})", flush=True)

    hits_b, hits_n, hits_a = [], [], []
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
                "market": market_map.get(code, "UNKNOWN"),
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
            if sig["above"]:
                hits_a.append(base)

        except Exception as e:
            print(f"[SKIP] {code} err={type(e).__name__}", flush=True)

        if i % SLEEP_EVERY == 0:
            time.sleep(SLEEP_SEC)

    # 전체에서 먼저 정리
    hits_b, hits_n, hits_a = sort_and_dedupe(hits_b, hits_n, hits_a)

    # 시장별로 분리 후 각 시장 내에서 다시 TOPN 보장
    kospi_b, kosdaq_b = split_by_market(hits_b)
    kospi_n, kosdaq_n = split_by_market(hits_n)
    kospi_a, kosdaq_a = split_by_market(hits_a)

    kospi_b = kospi_b[:TOPN]; kospi_n = kospi_n[:TOPN]; kospi_a = kospi_a[:TOPN]
    kosdaq_b = kosdaq_b[:TOPN]; kosdaq_n = kosdaq_n[:TOPN]; kosdaq_a = kosdaq_a[:TOPN]

    print(
        f"[RESULT] "
        f"KOSPI(b={len(kospi_b)}, n={len(kospi_n)}, a={len(kospi_a)}) "
        f"KOSDAQ(b={len(kosdaq_b)}, n={len(kosdaq_n)}, a={len(kosdaq_a)})",
        flush=True
    )

    subject, body = send_mail(kospi_b, kospi_n, kospi_a, kosdaq_b, kosdaq_n, kosdaq_a)
    _ = tg_send(subject + "\n" + body)
    print("[OK] done", flush=True)


if __name__ == "__main__":
    main()
