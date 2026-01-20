import os
import io
import zipfile
import math
import requests
import pandas as pd
from datetime import datetime, timezone, timedelta

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

KST = timezone(timedelta(hours=9))

# -------- Secrets / ENV --------
# 종목명 매핑 (KIS 마스터)
KOSPI_URL = os.getenv("KIS_KOSPI_MST_URL", "").strip()
KOSDAQ_URL = os.getenv("KIS_KOSDAQ_MST_URL", "").strip()

# 전종목 EOD(일별 종가/거래량) 다운로드 링크 (추가로 필요)
EOD_URL = os.getenv("KR_EOD_URL", "").strip()  # <-- 이거 1개만 추가

# 메일
SMTP_HOST = os.getenv("SMTP_HOST", "").strip()
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "").strip()
SMTP_PASS = os.getenv("SMTP_PASS", "").strip()
MAIL_TO = os.getenv("MAIL_TO", "").strip()

TOPN = int(os.getenv("TOPN", "30"))
VOL_MULT = float(os.getenv("VOL_MULT", "1.2"))
MIN_PRICE = float(os.getenv("MIN_PRICE", "0"))

# -------- Utils: load name map --------
def download_zip(url: str) -> bytes:
    if not url:
        raise RuntimeError("URL 누락")
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.content

def load_mst_map(zip_bytes: bytes) -> dict:
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
        name = line[6:40].strip()
        if name:
            m[code] = name
    return m

def get_name_map():
    kospi_map = load_mst_map(download_zip(KOSPI_URL))
    kosdaq_map = load_mst_map(download_zip(KOSDAQ_URL))
    return {**kospi_map, **kosdaq_map}

# -------- EOD loader (전종목 일괄) --------
def load_eod_df() -> pd.DataFrame:
    """
    기대 포맷(예시):
    date,code,close,volume
    2026-01-20,005930,72300,12345678
    ...
    """
    if not EOD_URL:
        raise RuntimeError("KR_EOD_URL(전종목 EOD 다운로드 링크) 누락")

    r = requests.get(EOD_URL, timeout=60)
    r.raise_for_status()

    # CSV면 그대로, zip이면 풀어서 첫 CSV 읽기
    content_type = r.headers.get("content-type", "").lower()
    data = r.content

    if data[:2] == b"PK":  # zip magic
        zf = zipfile.ZipFile(io.BytesIO(data))
        csv_name = None
        for n in zf.namelist():
            if n.lower().endswith(".csv"):
                csv_name = n
                break
        if csv_name is None:
            csv_name = zf.namelist()[0]
        csv_bytes = zf.read(csv_name)
        df = pd.read_csv(io.BytesIO(csv_bytes))
    else:
        df = pd.read_csv(io.BytesIO(data))

    # 컬럼 정규화
    df.columns = [c.strip().lower() for c in df.columns]
    need = {"date", "code", "close", "volume"}
    if not need.issubset(set(df.columns)):
        raise RuntimeError(f"EOD 컬럼이 필요합니다: {need} / 현재: {df.columns.tolist()}")

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    df["code"] = df["code"].astype(str).str.zfill(6)
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
    df = df.dropna(subset=["close", "volume"])
    return df

# -------- MA20 entry scan --------
def scan_ma20_entry(df: pd.DataFrame) -> pd.DataFrame:
    """
    df: date, code, close, volume (여러 날짜 포함, 종목별로 20일 이상 있어야 함)
    """
    df = df.sort_values(["code", "date"]).copy()

    # 종목별 rolling
    df["ma20"] = df.groupby("code")["close"].transform(lambda s: s.rolling(20).mean())
    df["vma20"] = df.groupby("code")["volume"].transform(lambda s: s.rolling(20).mean())

    # 어제/오늘 비교 위해 shift
    df["close_prev"] = df.groupby("code")["close"].shift(1)
    df["ma20_prev"] = df.groupby("code")["ma20"].shift(1)

    latest_date = df["date"].max()
    today = df[df["date"] == latest_date].copy()

    # 진입 조건
    today["entry"] = (today["close_prev"] < today["ma20_prev"]) & (today["close"] >= today["ma20"])
    today["volx"] = today["volume"] / today["vma20"]
    today["pct"] = (today["close"] / today["ma20"] - 1.0) * 100.0

    out = today[
        (today["entry"]) &
        (today["volx"] >= VOL_MULT) &
        (today["close"] >= MIN_PRICE)
    ].copy()

    out = out.dropna(subset=["ma20", "volx", "pct"])
    out["abs_pct"] = out["pct"].abs()
    out = out.sort_values("abs_pct", ascending=True).head(TOPN)
    out["date_str"] = out["date"].dt.strftime("%Y-%m-%d")
    return out[["code", "close", "ma20", "pct", "volx", "date_str"]]

# -------- Mail --------
def build_html_table(rows):
    if rows.empty:
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
    for i, r in enumerate(rows.itertuples(index=False), start=1):
        sign = "+" if r.pct >= 0 else ""
        trs.append(f"""
        <tr>
          <td style="padding:8px;border:1px solid #ddd;text-align:center;">{i}</td>
          <td style="padding:8px;border:1px solid #ddd;">{r.code} {r.name}</td>
          <td style="padding:8px;border:1px solid #ddd;text-align:right;">{int(r.close):,}</td>
          <td style="padding:8px;border:1px solid #ddd;text-align:right;">{r.ma20:.1f}</td>
          <td style="padding:8px;border:1px solid #ddd;text-align:right;">{sign}{r.pct:.2f}%</td>
          <td style="padding:8px;border:1px solid #ddd;text-align:right;">{r.volx:.2f}x</td>
          <td style="padding:8px;border:1px solid #ddd;text-align:center;">{r.date_str}</td>
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
        raise RuntimeError("SMTP 환경변수 누락")

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

def main():
    name_map = get_name_map()
    eod = load_eod_df()

    scanned = scan_ma20_entry(eod)
    scanned["name"] = scanned["code"].map(name_map).fillna("")

    ts = datetime.now(KST).strftime("%Y-%m-%d %H:%M KST")
    subject = f"[EOD] 한국주식 20일선 진입 요약 (장마감) - {ts[:10]}"

    rules = [
        "기준: 장 마감 종가(EOD) 기준",
        "대상: KOSPI + KOSDAQ 전종목",
        "조건: 어제 종가 < 어제 20MA AND 오늘 종가 ≥ 오늘 20MA",
        f"거래량 필터: 오늘 거래량 ≥ 20일 평균 거래량 × {VOL_MULT}",
        f"상위 {TOPN}종목"
    ]
    rules_html = "<br/>".join([f"- {x}" for x in rules])
    rules_text = "\n".join([f"- {x}" for x in rules])

    html = f"""
    <div style="font-family:Apple SD Gothic Neo, Malgun Gothic, Arial, sans-serif;line-height:1.6;">
      <h2 style="margin:0 0 8px 0;">{subject}</h2>
      <p style="margin:0 0 10px 0;">
        <b>기준 시각</b>: {ts}<br/>
        <b>대상</b>: KOSPI + KOSDAQ 전종목
      </p>

      <div style="padding:10px 12px;background:#f7f7f7;border:1px solid #e5e5e5;border-radius:8px;">
        <b>집계 기준</b><br/>{rules_html}
      </div>

      <h3 style="margin:18px 0 8px 0;">20일선 상향 진입 TOP {TOPN}</h3>
      {build_html_table(scanned)}

      <p style="margin-top:14px;color:#666;font-size:12px;">
        ※ 본 메일은 EOD 전종목 데이터 기반으로 장 마감 후 자동 생성·발송됩니다.
      </p>
    </div>
    """

    if scanned.empty:
        lines = "조건 충족 종목 없음"
    else:
        lines = "\n".join([
            f"{i:>2}. {r.code} {r.name} | 종가 {int(r.close):,} | 20MA {r.ma20:.1f} | {r.pct:+.2f}% | {r.volx:.2f}x | {r.date_str}"
            for i, r in enumerate(scanned.itertuples(index=False), start=1)
        ])

    text = f"""{subject}

[기준]
{rules_text}

[20일선 상향 진입 TOP {TOPN}]
{lines}
"""
    send_mail(subject, html, text)
    print(f"OK: mailed {len(scanned)} rows")

if __name__ == "__main__":
    main()
