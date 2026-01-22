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
MAIL_FROM = os.getenv("MAIL_FROM", SMTP_USER).strip()

# 메일 전송 방식 (465면 보통 SSL, 587이면 STARTTLS)
SMTP_SSL = os.getenv("SMTP_SSL", "").strip()  # "1"이면 강제 SSL

TOPN = int(os.getenv("TOPN", "30"))
MIN_PRICE = float(os.getenv("MIN_PRICE", "0"))

# 거래량 필터: 오늘 거래량 >= 20일 평균 거래량 * VOL_MULT
VOL_MULT = float(os.getenv("VOL_MULT", "1.0"))

# ✅ 근접(±%) 범위 (기본 ±0.5% = 0.005)
NEAR_PCT = float(os.getenv("NEAR_PCT", "0.005"))

# ✅ 기본은 0건이면 메일 안 보냄. (테스트/확인용으로만 1로)
SEND_EMPTY = os.getenv("SEND_EMPTY", "0").strip() == "1"

# 국내주식 기간별 시세(일) TR
TR_ID_CHART = os.getenv("KIS_TR_ID_CHART", "FHKST03010100").strip()

# 과호출 방지
SLEEP_EVERY = int(os.getenv("SLEEP_EVERY", "25"))
SLEEP_SEC = float(os.getenv("SLEEP_SEC", "0.25"))

# 업종 조회 TR(현재가) – 업종명(bstp_kor_isnm) 사용
TR_ID_PRICE = os.getenv("KIS_TR_ID_PRICE", "FHKST01010100").strip()

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
    고정폭 포맷 차이를 감안해, 코드/이름을 넉넉히 파싱
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

def kis_industry_name(token: str, code: str) -> str:
    """
    업종명(대분류, 한국어) 조회: output.bstp_kor_isnm
    (없는 경우 '기타')
    """
    url = f"{KIS_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-price"
    headers = {
        "authorization": f"Bearer {token}",
        "appkey": KIS_APPKEY,
        "appsecret": KIS_APPSECRET,
        "tr_id": TR_ID_PRICE,
    }
    params = {
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_INPUT_ISCD": code,
    }
    r = requests.get(url, headers=headers, params=params, timeout=10)
    r.raise_for_status()
    out = r.json().get("output", {}) or {}
    return (out.get("bstp_kor_isnm") or "기타").strip() or "기타"

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
# Signal: 20MA "돌파" + "근접"
# -----------------------------
def calc_signals(df: pd.DataFrame):
    """
    return dict with:
      - breakout (상향진입): (c1 < m1) and (c0 >= m0) and vol filter
      - near (근접): abs(c0/m0 - 1) <= NEAR_PCT and vol filter
    """
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

    volx = (v0 / vm0) if vm0 > 0 else 0.0
    if volx < VOL_MULT:
        return None

    pct = ((c0 / m0) - 1.0) * 100.0 if m0 else 0.0
    near = (abs((c0 / m0) - 1.0) <= NEAR_PCT) if m0 else False
    breakout = (c1 < m1) and (c0 >= m0)

    return {
        "close": float(c0),
        "ma20": float(m0),
        "pct": float(pct),
        "volx": float(volx),
        "date": df["date"].iloc[-1].strftime("%Y-%m-%d"),
        "is_breakout": bool(breakout),
        "is_near": bool(near),
    }

# -----------------------------
# Mail (HTML)
# -----------------------------
def build_html_table(rows, title: str):
    if not rows:
        return f"<p><b>{title}</b><br/>조건 충족 종목 없음</p>"

    thead = """
    <tr>
      <th style="padding:8px;border:1px solid #ddd;">#</th>
      <th style="padding:8px;border:1px solid #ddd;">종목</th>
      <th style="padding:8px;border:1px solid #ddd;">업종</th>
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
          <td style="padding:8px;border:1px solid #ddd;">{r.get("industry","기타")}</td>
          <td style="padding:8px;border:1px solid #ddd;text-align:right;">{r["close"]:,}</td>
          <td style="padding:8px;border:1px solid #ddd;text-align:right;">{r["ma20"]:.1f}</td>
          <td style="padding:8px;border:1px solid #ddd;text-align:right;">{sign}{r["pct"]:.2f}%</td>
          <td style="padding:8px;border:1px solid #ddd;text-align:right;">{r["volx"]:.2f}x</td>
          <td style="padding:8px;border:1px solid #ddd;text-align:center;">{r["date"]}</td>
        </tr>
        """)
    return f"""
    <h3 style="margin:18px 0 8px 0;">{title}</h3>
    <table style="border-collapse:collapse;width:100%;font-size:14px;">
      <thead>{thead}</thead>
      <tbody>{''.join(trs)}</tbody>
    </table>
    """

def group_by_industry(items: list[dict]) -> dict:
    """
    업종명 -> items
    * 업종별로 abs(pct) 작은 순으로 이미 정렬된 상태를 유지
    * 업종 섹션 순서는 '종목 수 많은 업종' 우선으로 보여줌
    """
    g = {}
    for it in items:
        ind = (it.get("industry") or "기타").strip() or "기타"
        g.setdefault(ind, []).append(it)
    # 섹션 정렬: 건수 desc, 이름 asc
    return dict(sorted(g.items(), key=lambda kv: (-len(kv[1]), kv[0])))

def send_mail(subject: str, html_body: str, text_body: str):
    if not (SMTP_HOST and SMTP_USER and SMTP_PASS and MAIL_TO):
        raise RuntimeError("SMTP 환경변수 누락(SMTP_HOST/USER/PASS/MAIL_TO)")

    msg = MIMEMultipart("alternative")
    msg["From"] = MAIL_FROM or SMTP_USER
    msg["To"] = MAIL_TO
    msg["Subject"] = subject

    msg.attach(MIMEText(text_body, "plain", "utf-8"))
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    use_ssl = False
    if SMTP_SSL:
        use_ssl = (SMTP_SSL == "1")
    else:
        use_ssl = (SMTP_PORT == 465)

    if use_ssl:
        with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=30) as server:
            server.login(SMTP_USER, SMTP_PASS)
            server.sendmail(MAIL_FROM or SMTP_USER, [MAIL_TO], msg.as_string())
    else:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASS)
            server.sendmail(MAIL_FROM or SMTP_USER, [MAIL_TO], msg.as_string())

# -----------------------------
# Main
# -----------------------------
def main():
    token = kis_token()
    codes, name_map = get_universe()
    universe_count = len(codes)

    breakout_hits = []
    near_hits = []

    industry_cache = {}  # code -> industry name

    for i, code in enumerate(codes, start=1):
        try:
            j = kis_daily_chart(token, code)
            df = parse_chart(j)
            sig = calc_signals(df)

            if sig:
                # ✅ 업종명은 "신호 후보"에만 조회 (호출량 절감) + 캐시
                if code not in industry_cache:
                    try:
                        industry_cache[code] = kis_industry_name(token, code)
                    except Exception:
                        industry_cache[code] = "기타"
                industry = industry_cache.get(code, "기타")

                base = {
                    "code": code,
                    "name": name_map.get(code, ""),
                    "industry": industry,
                    "close": sig["close"],
                    "ma20": sig["ma20"],
                    "pct": sig["pct"],
                    "volx": sig["volx"],
                    "date": sig["date"],
                }
                if sig["is_breakout"]:
                    breakout_hits.append(base)
                if sig["is_near"]:
                    near_hits.append(base)
        except Exception:
            pass

        if i % SLEEP_EVERY == 0:
            time.sleep(SLEEP_SEC)

    # 중복 제거: 돌파에 잡힌 애는 근접에서도 빠지게(메일이 깔끔해짐)
    breakout_codes = set([x["code"] for x in breakout_hits])
    near_hits = [x for x in near_hits if x["code"] not in breakout_codes]

    # 정렬
    # 돌파: MA대비 괴리율이 작은 순(진입 직후)
    breakout_hits.sort(key=lambda x: abs(x["pct"]))
    # 근접: 절대괴리율 작은 순
    near_hits.sort(key=lambda x: abs(x["pct"]))

    breakout_hits = breakout_hits[:TOPN]
    near_hits = near_hits[:TOPN]

    ts = datetime.now(KST).strftime("%Y-%m-%d %H:%M KST")
    subject = (
        f"[KIS] 20일선 돌파/근접 (장마감) "
        f"돌파{len(breakout_hits)} · 근접{len(near_hits)} - {ts[:10]}"
    )

    rules = [
        "기준: 장 마감 종가 기준",
        "대상: KOSPI + KOSDAQ 전종목",
        "돌파: 어제 종가 < 어제 20MA AND 오늘 종가 ≥ 오늘 20MA",
        f"근접: 오늘 종가가 오늘 20MA의 ±{NEAR_PCT*100:.2f}% 이내",
        f"거래량 필터: 오늘 거래량 ≥ 20일 평균 거래량 × {VOL_MULT}",
        f"각 그룹 상위 {TOPN}종목",
        "표시는 KIS 업종명(bstp_kor_isnm) 기준으로 자동 분리"
    ]

    if (not breakout_hits) and (not near_hits) and (not SEND_EMPTY):
        print("OK: mailed 0 rows (no candidates)")
        return

    # rows format
    def to_rows(items):
        rows = []
        for idx, h in enumerate(items, start=1):
            rows.append({
                "rank": idx,
                "code": h["code"],
                "name": h["name"],
                "industry": h.get("industry", "기타"),
                "close": int(round(h["close"])),
                "ma20": h["ma20"],
                "pct": h["pct"],
                "volx": h["volx"],
                "date": h["date"]
            })
        return rows

    rows_breakout_all = to_rows(breakout_hits)
    rows_near_all = to_rows(near_hits)

    # 업종별 그룹핑(표 섹션 분리)
    breakout_groups = group_by_industry(rows_breakout_all)
    near_groups = group_by_industry(rows_near_all)

    # HTML
    rules_html = "<br/>".join([f"- {x}" for x in rules])

    html_sections = []

    html_sections.append(f"<h2 style='margin:18px 0 8px 0;'>📈 20일선 돌파 (업종별, 최대 {TOPN})</h2>")
    if not rows_breakout_all:
        html_sections.append("<p>조건 충족 종목 없음</p>")
    else:
        for ind, rows in breakout_groups.items():
            html_sections.append(build_html_table(rows, f"{ind}"))

    html_sections.append(f"<h2 style='margin:24px 0 8px 0;'>👀 20일선 근접 (업종별, ±{NEAR_PCT*100:.2f}%, 최대 {TOPN})</h2>")
    if not rows_near_all:
        html_sections.append("<p>조건 충족 종목 없음</p>")
    else:
        for ind, rows in near_groups.items():
            html_sections.append(build_html_table(rows, f"{ind}"))

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

      {''.join(html_sections)}

      <div style="margin-top:16px;padding:10px 12px;border:1px solid #e5e5e5;border-radius:8px;">
        <b>해석 가이드</b><br/>
        - 돌파: 단기 추세 전환 후보(다음날 유지 여부 확인)<br/>
        - 근접: 다음날 장중 돌파/이탈 후보(관찰 리스트)<br/>
        - 거래량 배수가 높을수록 신뢰도 ↑
      </div>

      <p style="margin-top:14px;color:#666;font-size:12px;">
        ※ 본 메일은 한국투자증권(KIS) OpenAPI 기반으로 장 마감 후 자동 생성·발송됩니다.
      </p>
    </div>
    """

    # TEXT
    rules_text = "\n".join([f"- {x}" for x in rules])

    def fmt_group_text(title: str, grouped: dict) -> str:
        if not grouped:
            return f"[{title}]\n조건 충족 종목 없음\n"
        blocks = [f"[{title}]"]
        for ind, rows in grouped.items():
            blocks.append(f"\n■ {ind} ({len(rows)})")
            for r in rows:
                blocks.append(
                    f"{r['rank']:>2}. {r['code']} {r['name']} | 업종:{r.get('industry','기타')} | "
                    f"종가 {r['close']:,} | 20MA {r['ma20']:.1f} | {r['pct']:+.2f}% | "
                    f"{r['volx']:.2f}x | {r['date']}"
                )
        return "\n".join(blocks) + "\n"

    text = f"""{subject}

[기준 시각]
- {ts}

[대상]
- KOSPI + KOSDAQ 전종목 ({universe_count:,}종)

[집계 기준]
{rules_text}

{fmt_group_text(f"📈 20일선 돌파 (업종별, 최대 {TOPN})", breakout_groups)}

{fmt_group_text(f"👀 20일선 근접 (업종별, ±{NEAR_PCT*100:.2f}%, 최대 {TOPN})", near_groups)}

※ 본 메일은 한국투자증권(KIS) OpenAPI 기반으로 장 마감 후 자동 생성·발송됩니다.
"""

    send_mail(subject, html, text)
    mailed_count = len(rows_breakout_all) + len(rows_near_all)
    print(f"OK: mailed {mailed_count} rows (breakout={len(rows_breakout_all)}, near={len(rows_near_all)})")

if __name__ == "__main__":
    main()
