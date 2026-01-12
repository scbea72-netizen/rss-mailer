#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
rss_digest.py (fixed)
- Biztoc / jina.ai->biztoc links are blocked by 403/captcha and cannot be used reliably in automation.
- This script ONLY uses direct publisher RSS feeds and SKIPS any biztoc links if they appear.
- Sends a daily/periodic digest email via SMTP.

Requirements:
  pip install requests feedparser python-dateutil

Usage:
  python rss_digest.py

Recommended: set env vars:
  SMTP_HOST=smtp.hanmail.net
  SMTP_PORT=465
  SMTP_USER=your_id@hanmail.net
  SMTP_PASS=your_app_password_or_password
  MAIL_TO=your_id@hanmail.net
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
from typing import List, Dict, Any, Optional, Tuple

import requests
import feedparser
from dateutil import parser as dtparser

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
    # 필요하면 여기에 "원문 RSS"만 추가하세요.
]

# -----------------------------
# 2) SMTP / MAIL SETTINGS
# -----------------------------
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.hanmail.net")
SMTP_PORT = int(os.getenv("SMTP_PORT", "465"))  # SSL: 465, STARTTLS: 587
SMTP_USER = os.getenv("SMTP_USER", "")          # e.g. tactnet@hanmail.net
SMTP_PASS = os.getenv("SMTP_PASS", "")          # password (or app password)
MAIL_TO   = os.getenv("MAIL_TO", SMTP_USER)     # default: send to self
MAIL_FROM = os.getenv("MAIL_FROM", SMTP_USER)

# Digest subject prefix
SUBJECT_PREFIX = os.getenv("SUBJECT_PREFIX", "[RSS DIGEST]")

# Fetch limits
MAX_ITEMS_PER_FEED = int(os.getenv("MAX_ITEMS_PER_FEED", "30"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "15"))

# Only include items newer than N hours (optional)
MAX_AGE_HOURS = int(os.getenv("MAX_AGE_HOURS", "48"))

# Cache to prevent duplicates
CACHE_PATH = Path(os.getenv("CACHE_PATH", "sent_cache.json"))
CACHE_MAX_KEYS = int(os.getenv("CACHE_MAX_KEYS", "5000"))

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
        # proxy 형태로 biztoc이 들어가도 차단
        if JINA_PROXY_RE.search(url) and "biztoc.com" in url.lower():
            return True
        return False
    except Exception:
        return False

def resolve_final_url(url: str) -> str:
    """
    Resolve redirects (for normal publishers).
    NOTE: Biztoc is skipped before calling this.
    """
    try:
        # HEAD 먼저 (일부 사이트는 HEAD 막아서 GET으로 fallback)
        r = requests.head(url, allow_redirects=True, timeout=REQUEST_TIMEOUT, headers={
            "User-Agent": "Mozilla/5.0 (compatible; RSSDigest/1.0)"
        })
        if r.status_code >= 400 or not r.url:
            r = requests.get(url, allow_redirects=True, timeout=REQUEST_TIMEOUT, headers={
                "User-Agent": "Mozilla/5.0 (compatible; RSSDigest/1.0)"
            })
        return r.url or url
    except Exception:
        return url

def parse_entry_time(entry: Dict[str, Any]) -> Optional[datetime]:
    """
    Attempt to parse entry published/updated time.
    """
    for key in ("published", "updated", "created"):
        if key in entry and entry[key]:
            try:
                dt = dtparser.parse(entry[key])
                if not dt.tzinfo:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except Exception:
                continue

    # feedparser sometimes provides *_parsed
    for key in ("published_parsed", "updated_parsed"):
        if key in entry and entry[key]:
            try:
                # time.struct_time -> datetime
                dt = datetime.fromtimestamp(time.mktime(entry[key]), tz=timezone.utc)
                return dt
            except Exception:
                continue
    return None

def load_cache() -> Dict[str, float]:
    if CACHE_PATH.exists():
        try:
            data = json.loads(CACHE_PATH.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                # key -> timestamp
                return {k: float(v) for k, v in data.items()}
        except Exception:
            pass
    return {}

def save_cache(cache: Dict[str, float]) -> None:
    # trim
    if len(cache) > CACHE_MAX_KEYS:
        # 오래된 순으로 삭제
        items = sorted(cache.items(), key=lambda kv: kv[1])
        cache = dict(items[-CACHE_MAX_KEYS:])
    CACHE_PATH.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")

def make_key(feed_name: str, title: str, link: str) -> str:
    raw = f"{feed_name}|{title}|{link}".encode("utf-8", errors="ignore")
    return hashlib.sha256(raw).hexdigest()

def fetch_feed_items(feed: Dict[str, str]) -> List[Dict[str, Any]]:
    url = feed["url"]
    # 직접 RSS만 사용 (biztoc url이면 제외)
    if is_biztoc_url(url):
        return []

    parsed = feedparser.parse(url)
    items = []
    for entry in parsed.entries[:MAX_ITEMS_PER_FEED]:
        title = (entry.get("title") or "").strip()
        link  = (entry.get("link") or "").strip()

        # 어떤 피드는 origLink 같은게 있음
        # (가능하면 원문을 우선)
        for alt_key in ("feedburner_origlink", "origlink", "link"):
            alt = entry.get(alt_key)
            if alt and isinstance(alt, str) and alt.strip():
                link = alt.strip()
                break

        if not title or not link:
            continue

        # Biztoc 링크면 스킵 (핵심)
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
    out = []
    for it in items:
        dt = it.get("time")
        if not dt:
            out.append(it)  # 시간 없으면 포함
            continue
        if dt.timestamp() >= cutoff:
            out.append(it)
    return out

def build_email_html(items: List[Dict[str, Any]]) -> str:
    # 피드별 그룹
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for it in items:
        grouped.setdefault(it["feed"], []).append(it)

    # 최신순 정렬
    for k in grouped:
        grouped[k].sort(key=lambda x: (x["time"].timestamp() if x["time"] else 0), reverse=True)

    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    html = [f"<h2>{SUBJECT_PREFIX} {now}</h2>"]
    html.append("<p>※ Biztoc 링크는 403/캡차 차단으로 자동 스킵됩니다. 원문 RSS만 포함합니다.</p>")
    html.append("<hr/>")

    for feed_name, feed_items in grouped.items():
        html.append(f"<h3>{feed_name} ({len(feed_items)})</h3>")
        html.append("<ul>")
        for it in feed_items:
            title = escape_html(it["title"])
            link = it["link"]

            # 일반 사이트는 리다이렉트 최종 링크로 정리(선택)
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
        html.append("</ul>")
        html.append("<hr/>")

    return "\n".join(html)

def escape_html(s: str) -> str:
    return (
        s.replace("&", "&amp;")
         .replace("<", "&lt;")
         .replace(">", "&gt;")
         .replace('"', "&quot;")
         .replace("'", "&#39;")
    )

def send_mail(subject: str, html_body: str) -> None:
    if not SMTP_USER or not SMTP_PASS or not MAIL_TO:
        raise RuntimeError("SMTP_USER/SMTP_PASS/MAIL_TO 환경변수가 비어있습니다. 계정 정보를 설정하세요.")

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = MAIL_FROM
    msg["To"] = MAIL_TO

    part_html = MIMEText(html_body, "html", "utf-8")
    msg.attach(part_html)

    # SSL 465 기본
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

    # 1) fetch
    for feed in FEEDS:
        try:
            items = fetch_feed_items(feed)
            items = filter_recent(items)
            all_items.extend(items)
        except Exception:
            # feed 하나 죽어도 전체는 계속
            print(f"[WARN] feed failed: {feed.get('name')} ({feed.get('url')})")
            traceback.print_exc()

    # 2) dedupe by cache
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

    # 3) sort overall 최신순
    fresh.sort(key=lambda x: (x["time"].timestamp() if x["time"] else 0), reverse=True)

    # 4) build & send
    subject = f"{SUBJECT_PREFIX} {datetime.now().strftime('%Y-%m-%d')}"
    html = build_email_html(fresh)
    send_mail(subject, html)

    save_cache(cache)
    print(f"[OK] Sent {len(fresh)} items to {MAIL_TO}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
