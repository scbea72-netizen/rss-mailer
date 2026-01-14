#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
rss_digest.py (improved for GitHub Actions)
- Sends ONLY new items (dedupe via persistent cache)
- Skips Biztoc / r.jina.ai->biztoc links
- Optional: resolve final redirect URL (default OFF for speed/stability)

Requirements:
  pip install requests feedparser python-dateutil
"""

from __future__ import annotations

import os
import re
import json
import time
import hashlib
import traceback
from datetime import datetime, timezone
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib
from pathlib import Path
from typing import List, Dict, Any, Optional

import requests
import feedparser
from dateutil import parser as dtparser
from googletrans import Translator

# -----------------------------
# 1) RSS FEEDS (NO BIZTOC)
# -----------------------------
FEEDS: List[Dict[str, str]] = [
    {"name": "Reuters - Macro", "url": "https://www.reuters.com/rssFeed/macro"},
    {"name": "Reuters - World", "url": "https://www.reuters.com/world/rss"},
    {"name": "CNBC - Markets", "url": "https://www.cnbc.com/id/10000664/device/rss/rss.html"},
    {"name": "CNBC - Economy", "url": "https://www.cnbc.com/id/100003114/device/rss/rss.html"},
    {"name": "YNA - Market", "url": "https://www.yna.co.kr/rss/market.xml"},
    {"name": "YNA - Economy", "url": "https://www.yna.co.kr/rss/economy.xml"},
    {"name": "DART - Disclosures", "url": "https://opendart.fss.or.kr/api/rss.xml"},
]

# -----------------------------
# 2) SMTP / MAIL SETTINGS
# -----------------------------
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.hanmail.net")
SMTP_PORT = int(os.getenv("SMTP_PORT", "465"))  # SSL: 465, STARTTLS: 587
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASS = os.getenv("SMTP_PASS", "")
MAIL_TO   = os.getenv("MAIL_TO", SMTP_USER)
MAIL_FROM = os.getenv("MAIL_FROM", SMTP_USER)

SUBJECT_PREFIX = os.getenv("SUBJECT_PREFIX", "[RSS DIGEST]")

MAX_ITEMS_PER_FEED = int(os.getenv("MAX_ITEMS_PER_FEED", "30"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "15"))

# Only include items newer than N hours (optional)
MAX_AGE_HOURS = int(os.getenv("MAX_AGE_HOURS", "48"))

# IMPORTANT: cache must persist across runs (GitHub Actions cache will store this folder)
CACHE_PATH = Path(os.getenv("CACHE_PATH", ".cache/rss/sent_cache.json"))
CACHE_MAX_KEYS = int(os.getenv("CACHE_MAX_KEYS", "5000"))

# Optional: resolve final redirect URL (can be slow / sometimes blocked)
RESOLVE_FINAL_URL = os.getenv("RESOLVE_FINAL_URL", "0").strip() in ("1", "true", "TRUE", "yes", "YES")

# -----------------------------
# 3) BIZTOC / JINA BLOCK RULES
# -----------------------------
BIZTOC_HOST_RE = re.compile(r"(^|\.)biztoc\.com$", re.IGNORECASE)
JINA_PROXY_RE  = re.compile(r"^https?://r\.jina\.ai/https?://", re.IGNORECASE)

def is_biztoc_url(url: str) -> bool:
    try:
        from urllib.parse import urlparse
        u = urlparse(url)
        host = (u.hostname or "").lower()
        if BIZTOC_HOST_RE.search(host):
            return True
        if JINA_PROXY_RE.search(url) and "biztoc.com" in url.lower():
            return True
        return False
    except Exception:
        return False

def resolve_final_url(url: str) -> str:
    """Resolve redirects (for normal publishers). Biztoc is skipped before calling this."""
    if not RESOLVE_FINAL_URL:
        return url
    try:
        headers = {"User-Agent": "Mozilla/5.0 (compatible; RSSDigest/1.0)"}
        r = requests.head(url, allow_redirects=True, timeout=REQUEST_TIMEOUT, headers=headers)
        if r.status_code >= 400 or not r.url:
            r = requests.get(url, allow_redirects=True, timeout=REQUEST_TIMEOUT, headers=headers)
        return r.url or url
    except Exception:
        return url

def parse_entry_time(entry: Dict[str, Any]) -> Optional[datetime]:
    for key in ("published", "updated", "created"):
        if entry.get(key):
            try:
                dt = dtparser.parse(entry[key])
                if not dt.tzinfo:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except Exception:
                pass

    for key in ("published_parsed", "updated_parsed"):
        if entry.get(key):
            try:
                dt = datetime.fromtimestamp(time.mktime(entry[key]), tz=timezone.utc)
                return dt
            except Exception:
                pass
    return None

def load_cache() -> Dict[str, float]:
    if CACHE_PATH.exists():
        try:
            data = json.loads(CACHE_PATH.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                return {k: float(v) for k, v in data.items()}
        except Exception:
            pass
    return {}

def save_cache(cache: Dict[str, float]) -> None:
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    if len(cache) > CACHE_MAX_KEYS:
        items = sorted(cache.items(), key=lambda kv: kv[1])
        cache = dict(items[-CACHE_MAX_KEYS:])
    CACHE_PATH.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")

def make_key(feed_name: str, title: str, link: str) -> str:
    raw = f"{feed_name}|{title}|{link}".encode("utf-8", errors="ignore")
    return hashlib.sha256(raw).hexdigest()

def fetch_feed_items(feed: Dict[str, str]) -> List[Dict[str, Any]]:
    url = feed["url"]
    if is_biztoc_url(url):
        return []

    parsed = feedparser.parse(url)
    items: List[Dict[str, Any]] = []

    for entry in parsed.entries[:MAX_ITEMS_PER_FEED]:
        title = (entry.get("title") or "").strip()
        link  = (entry.get("link") or "").strip()

        # 일부 피드는 원문 링크 키가 따로 있음
        for alt_key in ("feedburner_origlink", "origlink", "link"):
            alt = entry.get(alt_key)
            if isinstance(alt, str) and alt.strip():
                link = alt.strip()
                break

        if not title or not link:
            continue

        if is_biztoc_url(link):
            continue

        dt = parse_entry_time(entry)
        items.append({
            "feed": feed["name"],
            "title": title,
            "link": link,
            "time": dt,
            "summary": (entry.get("summary") or "").strip()
        })

    return items

def filter_recent(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if MAX_AGE_HOURS <= 0:
        return items
    cutoff = datetime.now(timezone.utc).timestamp() - (MAX_AGE_HOURS * 3600)
    out: List[Dict[str, Any]] = []
    for it in items:
        dt = it.get("time")
        if not dt or dt.timestamp() >= cutoff:
            out.append(it)
    return out

def escape_html(s: str) -> str:
    return (
        s.replace("&", "&amp;")
         .replace("<", "&lt;")
         .replace(">", "&gt;")
         .replace('"', "&quot;")
         .replace("'", "&#39;")
    )

_TRANSLATOR = None

def is_korean_text(s: str) -> bool:
    return any('가' <= ch <= '힣' for ch in s)

def looks_english(s: str) -> bool:
    if not s:
        return False
    ascii_ratio = sum(1 for ch in s if ord(ch) < 128) / max(1, len(s))
    return ascii_ratio > 0.9 and not is_korean_text(s)

def translate_title_to_ko(title: str) -> str:
    global _TRANSLATOR
    if not looks_english(title):
        return title
    try:
        if _TRANSLATOR is None:
            _TRANSLATOR = Translator()
        out = _TRANSLATOR.translate(title, src="auto", dest="ko")
        ko = (out.text or "").strip()
        return ko if ko else title
    except Exception:
        return title

def build_email_html(items: List[Dict[str, Any]]) -> str:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for it in items:
        grouped.setdefault(it["feed"], []).append(it)

    for k in grouped:
        grouped[k].sort(key=lambda x: (x["time"].timestamp() if x["time"] else 0), reverse=True)

    now_local = datetime.now().strftime("%Y-%m-%d %H:%M")
    html = [f"<h2>{SUBJECT_PREFIX} {now_local}</h2>"]
    html.append("<p>※ Biztoc 링크는 403/캡차 차단으로 자동 스킵됩니다. 원문 RSS만 포함합니다.</p>")
    html.append("<hr/>")

    for feed_name, feed_items in grouped.items():
        html.append(f"<h3>{feed_name} ({len(feed_items)})</h3>")
        html.append("<ul>")
        for it in feed_items:
           original = it["title"]
ko_title = translate_title_to_ko(original)

if ko_title != original:
    title = escape_html(ko_title) + f"<br/><small style='color:#777'>({escape_html(original)})</small>"
else:
    title = escape_html(original)

            link = it["link"]
            final_link = resolve_final_url(link)

            t = it.get("time")
            t_str = ""
            if t:
                try:
                    t_str = t.astimezone().strftime("%Y-%m-%d %H:%M")
                except Exception:
                    t_str = ""
            meta = f" <small style='color:#666'>({t_str})</small>" if t_str else ""
            html.append(f"<li><a href='{final_link}'>{title}</a>{meta}</li>")
        html.append("</ul><hr/>")

    return "\n".join(html)

def send_mail(subject: str, html_body: str) -> None:
    if not SMTP_USER or not SMTP_PASS or not MAIL_TO:
        raise RuntimeError("SMTP_USER/SMTP_PASS/MAIL_TO 환경변수가 비어있습니다. GitHub Secrets를 확인하세요.")

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = MAIL_FROM
    msg["To"] = MAIL_TO
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    if SMTP_PORT == 465:
        with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=30) as server:
            server.login(SMTP_USER, SMTP_PASS)
            server.sendmail(MAIL_FROM, [MAIL_TO], msg.as_string())
    else:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as server:
            server.ehlo()
            server.starttls()
            server.login(SMTP_USER, SMTP_PASS)
            server.sendmail(MAIL_FROM, [MAIL_TO], msg.as_string())

def main() -> int:
    cache = load_cache()
    all_items: List[Dict[str, Any]] = []

    for feed in FEEDS:
        try:
            items = filter_recent(fetch_feed_items(feed))
            all_items.extend(items)
        except Exception:
            print(f"[WARN] feed failed: {feed.get('name')} ({feed.get('url')})")
            traceback.print_exc()

    fresh: List[Dict[str, Any]] = []
    now_ts = time.time()
    for it in all_items:
        key = make_key(it["feed"], it["title"], it["link"])
        if key in cache:
            continue
        cache[key] = now_ts
        fresh.append(it)

    if not fresh:
        print("[INFO] No new items to send.")
        save_cache(cache)
        return 0

    fresh.sort(key=lambda x: (x["time"].timestamp() if x["time"] else 0), reverse=True)

    subject = f"{SUBJECT_PREFIX} {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    html = build_email_html(fresh)
    send_mail(subject, html)

    save_cache(cache)
    print(f"[OK] Sent {len(fresh)} items to {MAIL_TO} | cache={CACHE_PATH}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
