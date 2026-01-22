# scripts/notify.py
from __future__ import annotations

import os
import smtplib
from email.mime.text import MIMEText

import requests


def send_telegram(text: str) -> None:
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

    if not token or not chat_id:
        print("[TG] skip (missing TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID)")
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": True}
    r = requests.post(url, data=payload, timeout=20)
    r.raise_for_status()
    print("[TG] sent")


def send_email(subject: str, body: str) -> None:
    """
    Gmail SMTP(권장): 앱 비밀번호 사용
    """
    smtp_host = os.environ.get("SMTP_HOST", "smtp.gmail.com").strip()
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))

    smtp_user = os.environ.get("SMTP_USER", "").strip()
    smtp_pass = os.environ.get("SMTP_PASS", "").strip()
    mail_to = os.environ.get("MAIL_TO", "").strip()

    if not smtp_user or not smtp_pass or not mail_to:
        print("[MAIL] skip (missing SMTP_USER / SMTP_PASS / MAIL_TO)")
        return

    msg = MIMEText(body, _charset="utf-8")
    msg["Subject"] = subject
    msg["From"] = smtp_user
    msg["To"] = mail_to

    with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as s:
        s.ehlo()
        s.starttls()
        s.login(smtp_user, smtp_pass)
        s.sendmail(smtp_user, [mail_to], msg.as_string())

    print("[MAIL] sent")
