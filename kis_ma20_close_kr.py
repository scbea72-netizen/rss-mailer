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

SMTP_HOST = os.getenv("SMTP_HOST", "").strip()
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "").strip()
SMTP_PASS = os.getenv("SMTP_PASS", "").strip()
MAIL_TO = os.getenv("MAIL_TO", "").strip()

TOPN = int(os.getenv("TOPN", "30"))
VOL_MULT = float(os.getenv("VOL_MULT", "1.2"))  # 오늘 거래량 >= 20일 평균 거래량 * VOL_MULT
MIN_PRICE = float(os.getenv("MIN_PRICE", "0"))

# 국내주식 기간별 시세(일/주/월/년) TR
TR_ID_CHART = os.getenv("KIS_TR_ID_CHART", "FHKST03010100").strip()

# 과호출 방지
SLEEP_EVERY = int(os.getenv("SLEEP_EVERY", "25"))
SLEEP_SEC = float(os.getenv("SLEEP_SEC", "0.25"))

# -----------------------------
# KIS
# -----------------------------
def kis_token() -> str:
    if not (KIS_APPKEY and KIS_APPSECRET):
        raise RuntimeError("KIS_APPKEY/KIS_APPSECRET 누락")
    url = f"{KIS_BASE_URL}/oauth2/tokenP"
    r = requests.post(url, json={
        "grant_type": "client_credentials",
        "appkey": KIS_APPKEY,
        "appsecret": KIS_APPSECRET
    }, timeout=20)
    r.raise_for_status()
    return r.json()["access_token"]

def download_mst_zip(url: str) -> bytes:
    if not url:
        raise RuntimeError("KIS_KOSPI_MST_URL 또는 KIS_KOSDAQ_MST_URL 누락")
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.content

def load_mst_map(zip_bytes: bytes) -> dict:
    """
    mst zip -> {code: name}
    NOTE: mst 고정폭 포맷은 버전에 따라 조금 다를 수 있어,
          안전하게 '앞 6자리 코드 + 이어지는 종목명 구간'을 넓게 읽습니다.
    """
    zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
    mst_name = None
    for n in zf.namelist():
        if n.lower().endswith(".mst"):
            mst_name = n
            break
    if mst_name is None:
        mst_name = zf.namelist()[0]

    raw = zf.read(mst_name)
    text = raw.decode("cp949", errors="ignore")

    m = {}
    for line in text.splitlines():
        if len(line) < 10:
            continue
        code = line[:6]
        if not code.isdigit():
            continue
        # 종목명 위치는 환경마다 다를 수 있어 넉넉히 슬라이스
        # 보통 코드 뒤에 한글 종목명이 포함됨
        name_guess = line[6:40].strip()
        if name_guess:
            m[code] = name_guess
    return m

def get_universe():
    kospi_map = load_mst_map(download_mst_zip(KOSPI_URL))
    kosdaq_map = load_mst_map(download_mst_zip(KOSDAQ_URL))
    name_map = {**kospi_map, **kosdaq_map}
    codes = sorted(name_map.keys())
    return codes, name_map

def kis_daily_chart(token: str, code: str):
    url = f"{KIS_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
    headers = {
        "authorization": f"Bearer {token}",
        "appkey": KIS_APPKEY,
        "appsecret": KIS_APPSECRET,
        "tr_id": TR_ID_CHART,
    }
    params = {
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_INPUT_ISCD": code,
        "FID_PERIOD_DIV_CODE": "D",
        "FID_ORG_ADJ_PRC": "0",
    }
    r = requests.get(url, headers=headers, params=params, timeout=20)
    r.raise_for_status()
    return r.json()

def parse_chart(j) -> pd.DataFrame:
    output = j.get("output2") or j.get("output1") or j.get("output") or []
    if not isinstance(output, list) or not output:
        return pd.DataFrame()

    rows = []
    for it in output:
        date = it.get("stck_bsop_date") or it.get("bsop_date") or it.get("date")
        close = it.get("stck_clpr") or it.get("close") or it.get("clpr")
        vol = it.get("acml_vol") or it.get("volume") or it.get("vol")
        if date is None or close is None or vol is None:
            continue
        try:
            rows.append({"date": str(date), "close": float(close), "volume": float(vol)})
        except Exception:
            continue

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    return df

# -----------------------------
# Signal: 20MA "상향 진입"
# -----------------------------
def calc_entry(df: pd.DataFrame):
    if df is None or df.empty or len(df) < 25:
        return None

    close = df["close"].astype(float)
    vol = df["volume"].astype(float)

    ma20 = close.rolling(20).mean()
    vma20 = vol.rolling(20).mean()

    c0, c1 = close.iloc[-1], close.iloc[-2]
    m0, m1 = ma20.iloc[-1], ma20.iloc[-2]
    v0, vm0 = vol.iloc[-1], vma20.iloc[-1]

    if any(math.isnan(x) for x in [c0, c1, m0, m1, v0, vm0]):
        return None
    if c0 < MIN_PRICE:
        return None

    # 상향 진입(어제는 아래, 오늘은 MA20 이상)
    entry = (c1 < m1) and (c0 >= m0)
    if not entry:
        return None

    volx = (v0 / vm0) if vm0 > 0 else 0.0
    if volx < VOL_MULT:
        return None

    pct = ((c0 / m0) - 1.0) * 100.0 if m0 else 0.0
    return {
        "close": float(c0),
        "ma20": float(m0),
        "pct": float(pct),
        "volx": float(volx),
        "date": df["date"].iloc[-1].strftime("%Y-%m-%d")
    }

# -----------------------------
# Mail (HTML)
# -----------------------------
def build_html_table(rows):
    if not rows:
        return "<p>조건 충족 종목 없음</p>"

    thead = """
    <tr>
      <th style="padding:8px;border:1px solid #ddd;">#</th>
      <th style="padding:8px;border:1px solid #ddd;">종목</th>
      <th style="padding:8px;border:1px solid #ddd;">종가</th>
      <th style="padding:8px;border:1px solid #ddd;">20MA</th>
      <th style="padding:8px;border:1px solid #ddd;">MA대비</th>
      <th style="padding:8px;border:1px solid #ddd;">거래량배수</th>
      <th style="padding:8px;border:1px solid #ddd;">기준일</th>
    </tr>
    """
    trs = []
    for r in rows:
        sign = "+" if r["pct"] >= 0 else ""
        trs.append(f"""
        <tr>
          <td style="padding:8px;border:1px solid #ddd;text-align:center;">{r["rank"]}</td>
          <td style="padding:8px;border:1px solid #ddd;">{r["code"]} {r["name"]}</td>
          <td style="padding:8px;border:1px solid #ddd;text-align:right;">{r["close"]:,}</td>
          <td style="padding:8px;border:1px solid #ddd;text-align:right;">{r["ma20"]:.1f}</td>
          <td style="padding:8px;border:1px solid #ddd;text-align:right;">{sign}{r["pct"]:.2f}%</td>
          <td style="padding:8px;border:1px solid #ddd;text-align:right;">{r["volx"]:.2f}x</td>
          <td style="padding:8px;border:1px solid #ddd;text-align:center;">{r["date"]}</td>
        </tr>
        """)
    return f"""
    <table style="border-collapse:collapse;width:100%;font-size:14px;">
      <thead>{thead}</thead>
      <tbody>{''.join(trs)}</tbody>
    </table>
    """

def send_mail(subject: str, html_body: str, text_body: str):
    if not (SMTP_HOST and SMTP_USER and SMTP_PASS and MAIL_TO):
        raise RuntimeError("SMTP 환경변수 누락(SMTP_HOST/USER/PASS/MAIL_TO)")

    msg = MIMEMultipart("alternative")
    msg["From"] = SMTP_USER
    msg["To"] = MAIL_TO
    msg["Subject"] = subject

    msg.attach(MIMEText(text_body, "plain", "utf-8"))
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as server:
        server.starttls()
        server.login(SMTP_USER, SMTP_PASS)
        server.sendmail(SMTP_USER, [MAIL_TO], msg.as_string())

# -----------------------------
# Main
# -----------------------------
def main():
    token = kis_token()
    codes, name_map = get_universe()
    universe_count = len(codes)

    hits = []
    for i, code in enumerate(codes, start=1):
        try:
            j = kis_daily_chart(token, code)
            df = parse_chart(j)
            sig = calc_entry(df)
            if sig:
                hits.append({
                    "code": code,
                    "name": name_map.get(code, ""),
                    **sig
                })
        except Exception:
            pass

        if i % SLEEP_EVERY == 0:
            time.sleep(SLEEP_SEC)

    # 정렬: MA20과의 절대 괴리율이 작은(=진입 직후에 가까운) 순으로
    hits.sort(key=lambda x: abs(x["pct"]))
    hits = hits[:TOPN]

    ts = datetime.now(KST).strftime("%Y-%m-%d %H:%M KST")
    subject = f"[KIS] 한국주식 20일선 진입 요약 (장마감) - {ts[:10]}"

    rules = [
        "기준: 장 마감 종가 기준",
        "대상: KOSPI + KOSDAQ 전종목",
        "조건: 어제 종가 < 어제 20MA AND 오늘 종가 ≥ 오늘 20MA",
        f"거래량 필터: 오늘 거래량 ≥ 20일 평균 거래량 × {VOL_MULT}",
        f"상위 {TOPN}종목"
    ]

    # rows format
    rows = []
    for idx, h in enumerate(hits, start=1):
        rows.append({
            "rank": idx,
            "code": h["code"],
            "name": h["name"],
            "close": int(round(h["close"])),
            "ma20": h["ma20"],
            "pct": h["pct"],
            "volx": h["volx"],
            "date": h["date"]
        })

    # HTML
    rules_html = "<br/>".join([f"- {x}" for x in rules])
    html = f"""
    <div style="font-family:Apple SD Gothic Neo, Malgun Gothic, Arial, sans-serif;line-height:1.6;">
      <h2 style="margin:0 0 8px 0;">{subject}</h2>
      <p style="margin:0 0 10px 0;">
        <b>기준 시각</b>: {ts}<br/>
        <b>대상</b>: KOSPI + KOSDAQ 전종목 ({universe_count:,}종)
      </p>

      <div style="padding:10px 12px;background:#f7f7f7;border:1px solid #e5e5e5;border-radius:8px;">
        <b>집계 기준</b><br/>{rules_html}
      </div>

      <h3 style="margin:18px 0 8px 0;">20일선 상향 진입 TOP {TOPN}</h3>
      {build_html_table(rows)}

      <div style="margin-top:16px;padding:10px 12px;border:1px solid #e5e5e5;border-radius:8px;">
        <b>해석 가이드</b><br/>
        - 20일선 진입은 단기 추세 전환 후보 신호<br/>
        - 거래량 배수가 높을수록 신뢰도 ↑<br/>
        - 다음 관찰: 20MA 재이탈 여부 / 다음날 연속 양봉 여부
      </div>

      <p style="margin-top:14px;color:#666;font-size:12px;">
        ※ 본 메일은 한국투자증권(KIS) OpenAPI 기반으로 장 마감 후 자동 생성·발송됩니다.
      </p>
    </div>
    """

    # TEXT
    rules_text = "\n".join([f"- {x}" for x in rules])
    if rows:
        lines = "\n".join([
            f"{r['rank']:>2}. {r['code']} {r['name']} | 종가 {r['close']:,} | 20MA {r['ma20']:.1f} | {r['pct']:+.2f}% | {r['volx']:.2f}x | {r['date']}"
            for r in rows
        ])
    else:
        lines = "조건 충족 종목 없음"

    text = f"""{subject}

[기준 시각]
- {ts}

[대상]
- KOSPI + KOSDAQ 전종목 ({universe_count:,}종)

[집계 기준]
{rules_text}

[20일선 상향 진입 TOP {TOPN}]
{lines}

※ 본 메일은 한국투자증권(KIS) OpenAPI 기반으로 장 마감 후 자동 생성·발송됩니다.
"""

    send_mail(subject, html, text)
    print(f"OK: mailed {len(rows)} rows")

if __name__ == "__main__":
    main()
