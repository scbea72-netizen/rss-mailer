#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations
import os, re, json, time, hashlib, traceback, smtplib
from datetime import datetime, timezone
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pathlib import Path
from typing import List, Dict, Any, Optional

import requests, feedparser
from dateutil import parser as dtparser
from googletrans import Translator

# -----------------------------
# RSS FEEDS
# -----------------------------
FEEDS = [
    {"name": "Reuters - Macro", "url": "https://www.reuters.com/rssFeed/macro"},
    {"name": "Reuters - World", "url": "https://www.reuters.com/world/rss"},
    {"name": "CNBC - Markets", "url": "https://www.cnbc.com/id/10000664/device/rss/rss.html"},
    {"name": "CNBC - Economy", "url": "https://www.cnbc.com/id/100003114/device/rss/rss.html"},
    {"name": "YNA - Market", "url": "https://www.yna.co.kr/rss/market.xml"},
    {"name": "YNA - Economy", "url": "https://www.yna.co.kr/rss/economy.xml"},
    {"name": "DART - Disclosures", "url": "https://opendart.fss.or.kr/api/rss.xml"},
]

# -----------------------------
# SMTP
# -----------------------------
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.hanmail.net")
SMTP_PORT = int(os.getenv("SMTP_PORT", "465"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASS = os.getenv("SMTP_PASS", "")
MAIL_TO   = os.getenv("MAIL_TO", SMTP_USER)
MAIL_FROM = os.getenv("MAIL_FROM", SMTP_USER)
SUBJECT_PREFIX = "[RSS DIGEST]"

CACHE_PATH = Path(".cache/rss/sent_cache.json")
CACHE_MAX_KEYS = 5000
MAX_ITEMS_PER_FEED = 30
MAX_AGE_HOURS = 48
REQUEST_TIMEOUT = 15

# -----------------------------
# UTILS
# -----------------------------
def load_cache():
    if CACHE_PATH.exists():
        return json.loads(CACHE_PATH.read_text(encoding="utf-8"))
    return {}

def save_cache(cache):
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    CACHE_PATH.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")

def make_key(feed, title, link):
    return hashlib.sha256(f"{feed}|{title}|{link}".encode()).hexdigest()

def escape_html(s):
    return s.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

def looks_english(s):
    return sum(ord(c)<128 for c in s)/max(1,len(s)) > 0.9

_translator = None
def translate_title_to_ko(t):
    global _translator
    if not looks_english(t):
        return t
    if not _translator:
        _translator = Translator()
    try:
        return _translator.translate(t, dest="ko").text
    except:
        return t

# -----------------------------
# HTML BUILDER (정상 버전)
# -----------------------------
def build_email_html(items):
    grouped = {}
    for it in items:
        grouped.setdefault(it["feed"], []).append(it)

    html = [f"<h2>{SUBJECT_PREFIX} {datetime.now().strftime('%Y-%m-%d %H:%M')}</h2><hr/>"]

    for feed_name, feed_items in grouped.items():
        html.append(f"<h3>{feed_name}</h3><ul>")

        for it in feed_items:
            original = it["title"]
            ko = translate_title_to_ko(original)

            if ko != original:
                title = escape_html(ko) + f"<br><small>({escape_html(original)})</small>"
            else:
                title = escape_html(original)

            link = it["link"]
            html.append(f"<li><a href='{link}'>{title}</a></li>")

        html.append("</ul><hr/>")

    return "\n".join(html)

# -----------------------------
# SEND
# -----------------------------
def send_mail(subject: str, html_body: str) -> None:
    if not SMTP_USER or not SMTP_PASS or not MAIL_TO:
        raise RuntimeError("SMTP_USER/SMTP_PASS/MAIL_TO 환경변수가 비어있습니다.")

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = MAIL_FROM
    msg["To"] = MAIL_TO
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    # 465 = SSL / 그 외 = STARTTLS
    if SMTP_PORT == 465:
        with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=30) as s:
            s.login(SMTP_USER, SMTP_PASS)
            s.sendmail(MAIL_FROM, [MAIL_TO], msg.as_string())
    else:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as s:
            s.ehlo()
            s.starttls()
            s.ehlo()
            s.login(SMTP_USER, SMTP_PASS)
            s.sendmail(MAIL_FROM, [MAIL_TO], msg.as_string())
# -----------------------------
# MAIN
# -----------------------------
def main():
    cache = load_cache()
    fresh = []

    for feed in FEEDS:
        parsed = feedparser.parse(feed["url"])
        for e in parsed.entries[:MAX_ITEMS_PER_FEED]:
            title = e.get("title","").strip()
            link = e.get("link","").strip()
            if not title or not link:
                continue
            key = make_key(feed["name"], title, link)
            if key not in cache:
                cache[key] = time.time()
                fresh.append({"feed":feed["name"],"title":title,"link":link})

    if not fresh:
        print("No new items.")
        save_cache(cache)
        return

    html = build_email_html(fresh)
    send_mail(SUBJECT_PREFIX, html)
    save_cache(cache)
    print("OK")

if __name__ == "__main__":
    main()
