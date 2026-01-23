# === merge_and_send.py ===
# 분할 스캔 결과(JSON) 병합 → 정렬 → 메일 1회 + 텔레그램 1회
# 엑셀(xlsx) 첨부 포함

import os
import glob
import json
import pandas as pd
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from datetime import datetime

# =========================
# ENV
# =========================
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com").strip()
SMTP_PORT = int(os.getenv("SMTP_PORT", "465"))
SMTP_USER = os.getenv("GMAIL_USER", "").strip()
SMTP_PASS = os.getenv("GMAIL_APP_PASS", "").strip()
MAIL_TO_RAW = (os.getenv("HANMAIL_TO") or "").strip()

TG_TOKEN = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
TG_CHAT_ID = (os.getenv("TELEGRAM_CHAT_ID") or "").strip()

TOPN = int(os.getenv("TOPN", "30"))

# =========================
# UTIL
# =========================
def parse_recipients(raw: str):
    raw = (raw or "").replace(";", ",")
    return [x.strip() for x in raw.split(",") if x.strip()]

def must_env():
    miss = []
    for k in ["GMAIL_USER", "GMAIL_APP_PASS", "HANMAIL_TO"]:
        if not os.getenv(k):
            miss.append(k)
    if miss:
        raise RuntimeError("Missing ENV: " + ", ".join(miss))

# =========================
# TELEGRAM
# =========================
def tg_send(text: str):
    if not (TG_TOKEN and TG_CHAT_ID):
        print("[TG] token/chat_id missing -> skip", flush=True)
        return
    import requests
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    # 길면 분할
    s = text or ""
    while s:
        chunk = s[:3900]
        s = s[3900:]
        requests.post(url, data={"chat_id": TG_CHAT_ID, "text": chunk}, timeout=15)

# =========================
# LOAD & MERGE
# =========================
def load_all_results():
    files = glob.glob("output/**/result_part_*.json", recursive=True)
    rows = []
    for f in files:
        try:
            with open(f, "r", encoding="utf-8") as fp:
                rows.extend(json.load(fp))
        except Exception as e:
            print("[WARN] skip file:", f, e, flush=True)
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    return df

def prioritize_and_trim(df: pd.DataFrame):
    if df.empty:
        return df

    # 우선순위: breakout > near > above
    def rank(row):
        if row.get("breakout"): return 0
        if row.get("near"): return 1
        if row.get("above"): return 2
        return 9

    df["_rank"] = df.apply(rank, axis=1)
    # pct가 있으면 큰 순
    if "pct" in df.columns:
        df = df.sort_values(by=["_rank", "pct"], ascending=[True, False])
    else:
        df = df.sort_values(by=["_rank"])

    # 종목 중복 제거 (code 기준)
    df = df.drop_duplicates(subset=["code"], keep="first")

    # 시장별 TOPN
    out = []
    for mkt in ["KOSPI", "KOSDAQ"]:
        d = df[df.get("market") == mkt].head(TOPN)
        out.append(d)
    return pd.concat(out, ignore_index=True)

# =========================
# FORMAT
# =========================
def fmt_text(df: pd.DataFrame):
    if df.empty:
        return "📉 오늘 조건 충족 종목 없음"

    lines = []
    for mkt in ["KOSPI", "KOSDAQ"]:
        sub = df[df.get("market") == mkt]
        if sub.empty:
            continue
        lines.append(f"[{mkt}]")
        for _, r in sub.iterrows():
            tag = "돌파" if r.get("breakout") else ("근접" if r.get("near") else "유지")
            pct = f"{r.get('pct', 0):+.2f}%" if "pct" in r else ""
            volx = f"{r.get('volx', 0):.2f}x" if "volx" in r else ""
            lines.append(
                f"- {r.get('code')} {r.get('name','')} | {r.get('industry','')} | {tag} {pct} {volx}"
            )
        lines.append("")
    return "\n".join(lines).strip()

# =========================
# MAIL (엑셀 첨부)
# =========================
def send_mail(subject: str, body: str, attach_path: str | None):
    to_list = parse_recipients(MAIL_TO_RAW)
    if not to_list:
        raise RuntimeError("HANMAIL_TO invalid")

    msg = MIMEMultipart()
    msg["Subject"] = subject
    msg["From"] = SMTP_USER
    msg["To"] = ", ".join(to_list)

    msg.attach(MIMEText(body, "plain", "utf-8"))

    if attach_path and os.path.exists(attach_path):
        with open(attach_path, "rb") as f:
            part = MIMEApplication(f.read(), Name=os.path.basename(attach_path))
        part["Content-Disposition"] = f'attachment; filename="{os.path.basename(attach_path)}"'
        msg.attach(part)

    with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT) as s:
        s.login(SMTP_USER, SMTP_PASS)
        s.sendmail(SMTP_USER, to_list, msg.as_string())

    print("[MAIL] sent ok", flush=True)

# =========================
# MAIN
# =========================
def main():
    must_env()

    df_all = load_all_results()
    df_final = prioritize_and_trim(df_all)

    # 엑셀 생성
    os.makedirs("output", exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    xlsx_path = f"output/MA20_signal_{ts}.xlsx"
    if not df_final.empty:
        df_final.to_excel(xlsx_path, index=False)
    else:
        # 빈 파일도 생성
        pd.DataFrame(columns=["market","code","name","industry","pct","volx"]).to_excel(xlsx_path, index=False)

    subject = f"[KIS] MA20 종가 시그널 ({ts})"
    body = fmt_text(df_final)

    send_mail(subject, body, xlsx_path)
    tg_send(subject + "\n" + body)

    print("[OK] merge & send done", flush=True)

if __name__ == "__main__":
    main()
