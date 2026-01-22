#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
rss_digest.py (RSS 뉴스 메일러 – 최종 안정판)

정책
- KR: 제목 그대로
- US / JP: 제목만 한글 번역 (MyMemory 무료)
- 본문 번역 없음 (링크만)
- 번역 실패해도 메일은 무조건 발송
"""

from __future__ import annotations

import os, re, json, time, hashlib, traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any, Optional
from urllib.parse import urlparse, urlunparse
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib

import requests
import feedparser
from dateutil import parser as dtparser

# =====================
# 0. ENV
# =====================
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "15"))
MAX_ITEMS_PER_FEED = int(os.getenv("MAX_ITEMS_PER_FEED", "30"))
MAX_AGE_HOURS = int(os.getenv("MAX_AGE_HOURS", "48"))

MAX_US = int(os.getenv("MAX_US", "25"))
MAX_KR = int(os.getenv("MAX_KR", "25"))
MAX_JP = int(os.getenv("MAX_JP", "25"))
MAX_ITEMS_PER_EMAIL = int(os.getenv("MAX_ITEMS_PER_EMAIL", "120"))

TITLE_TRANSLATE = os.getenv("TITLE_TRANSLATE", "1") == "1"
TRANSLATE_SLEEP_SECONDS = float(os.getenv("TRANSLATE_SLEEP_SECONDS", "1.0"))

USER_AGENT = os.getenv(
    "USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# =====================
# 1. HTTP
# =====================
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": USER_AGENT,
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,ja;q=0.7",
})

# =====================
# 2. FEEDS
# =====================
FEEDS = [
    {"category": "US", "name": "Reuters Macro", "url": "https://www.reuters.com/rssFeed/macro"},
    {"category": "US", "name": "Reuters World", "url": "https://www.reuters.com/world/rss"},
    {"category": "US", "name": "CNBC Markets", "url": "https://www.cnbc.com/id/10000664/device/rss/rss.html"},

    {"category": "KR", "name": "연합뉴스 시장", "url": "https://www.yna.co.kr/rss/market.xml"},
    {"category": "KR", "name": "연합뉴스 경제", "url": "https://www.yna.co.kr/rss/economy.xml"},

    {"category": "JP", "name": "NHK Business", "url": "https://www3.nhk.or.jp/rss/news/cat5.xml"},
    {"category": "JP", "name": "Reuters JP", "url": "https://feeds.reuters.com/reuters/JPbusinessNews"},
    {"category": "JP", "name": "Nikkei", "url": "https://www.nikkei.com/rss/news/cat0.xml"},
]

# =====================
# 3. SMTP
# =====================
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.daum.net")
SMTP_PORT = int(os.getenv("SMTP_PORT", "465"))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")
MAIL_TO = os.getenv("MAIL_TO", SMTP_USER)
MAIL_FROM = os.getenv("MAIL_FROM", SMTP_USER)
SUBJECT_PREFIX = os.getenv("SUBJECT_PREFIX", "[RSS]")

# =====================
# 4. CACHE
# =====================
CACHE_PATH = Path(".cache/rss/sent.json")
TITLE_CACHE_PATH = Path(".cache/rss/title.json")

def load_json(p: Path) -> Dict:
    if p.exists():
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except:
            pass
    return {}

def save_json(p: Path, d: Dict):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(d, ensure_ascii=False, indent=2), encoding="utf-8")

# =====================
# 5. UTIL
# =====================
def canonical(url: str) -> str:
    u = urlparse(url)
    qs = "&".join(
        p for p in u.query.split("&")
        if not p.lower().startswith("utm_")
    )
    return urlunparse(u._replace(query=qs, fragment=""))

def parse_time(e) -> Optional[datetime]:
    for k in ("published", "updated"):
        if e.get(k):
            try:
                d = dtparser.parse(e[k])
                return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
            except:
                pass
    return None

def has_ko(s: str) -> bool:
    return any("가" <= c <= "힣" for c in s)

def looks_ja(s: str) -> bool:
    return any(0x3040 <= ord(c) <= 0x30ff for c in s)

# =====================
# 6. TRANSLATE
# =====================
def translate_title(text: str, cache: Dict[str, str]) -> str:
    if has_ko(text):
        return text

    src = "ja" if looks_ja(text) else "en"
    key = f"{src}|{text}"
    if key in cache:
        return cache[key]

    try:
        r = SESSION.get(
            "https://api.mymemory.translated.net/get",
            params={"q": text, "langpair": f"{src}|ko"},
            timeout=REQUEST_TIMEOUT
        )
        out = r.json().get("responseData", {}).get("translatedText", text)
    except:
        out = text

    cache[key] = out
    time.sleep(TRANSLATE_SLEEP_SECONDS)
    return out

# =====================
# 7. FETCH
# =====================
def fetch(feed):
    items = []
    try:
        parsed = feedparser.parse(
            feed["url"],
            request_headers={"User-Agent": USER_AGENT}
        )
        for e in parsed.entries[:MAX_ITEMS_PER_FEED]:
            if not e.get("title") or not e.get("link"):
                continue
            items.append({
                "category": feed["category"],
                "feed": feed["name"],
                "title": e.title.strip(),
                "link": canonical(e.link.strip()),
                "time": parse_time(e)
            })
    except Exception:
        traceback.print_exc()
    return items

# =====================
# 8. MAIL HTML
# =====================
def build_html(items, title_cache):
    out = [f"<h2>{SUBJECT_PREFIX} {datetime.now().strftime('%Y-%m-%d %H:%M')}</h2><hr/>"]
    for cat in ("US", "KR", "JP"):
        group = [x for x in items if x["category"] == cat]
        if not group:
            continue
        out.append(f"<h3>[{cat}]</h3><ul>")
        for it in group:
            title = it["title"]
            if cat != "KR" and TITLE_TRANSLATE:
                title = translate_title(title, title_cache)
            out.append(f"<li><a href='{it['link']}'>{title}</a></li>")
        out.append("</ul>")
    return "\n".join(out)

# =====================
# 9. SEND
# =====================
def send(subject, html):
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = MAIL_FROM
    msg["To"] = MAIL_TO
    msg.attach(MIMEText(html, "html", "utf-8"))

    with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT) as s:
        s.login(SMTP_USER, SMTP_PASS)
        s.sendmail(MAIL_FROM, [MAIL_TO], msg.as_string())

# =====================
# 10. MAIN
# =====================
def main():
    sent = load_json(CACHE_PATH)
    title_cache = load_json(TITLE_CACHE_PATH)

    items = []
    for f in FEEDS:
        items.extend(fetch(f))

    fresh = []
    for it in items:
        key = hashlib.sha1(f"{it['title']}{it['link']}".encode()).hexdigest()
        if key in sent:
            continue
        sent[key] = time.time()
        fresh.append(it)

    fresh.sort(key=lambda x: x["time"] or datetime.min, reverse=True)
    fresh = (
        [x for x in fresh if x["category"] == "US"][:MAX_US] +
        [x for x in fresh if x["category"] == "KR"][:MAX_KR] +
        [x for x in fresh if x["category"] == "JP"][:MAX_JP]
    )

    if not fresh:
        print("NO NEW ITEMS")
        return

    html = build_html(fresh, title_cache)
    send(f"{SUBJECT_PREFIX} {datetime.now().strftime('%Y-%m-%d %H:%M')}", html)

    save_json(CACHE_PATH, sent)
    save_json(TITLE_CACHE_PATH, title_cache)

    print(f"SENT {len(fresh)} ITEMS")

if __name__ == "__main__":
    main()
