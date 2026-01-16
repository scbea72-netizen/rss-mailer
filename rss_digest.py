#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
rss_digest.py (무료·안정 / 제목만 한글 처리)

동작 규칙
- KR(한국): 제목 그대로
- US/JP(미국/일본): 제목만 한글화(용어 치환 기반, 무료)
- 본문은 원문 유지
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
from urllib.parse import urlparse, urlunparse

import requests
import feedparser
from dateutil import parser as dtparser

# -----------------------------
# 0) Runtime knobs (ENV)
# -----------------------------
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "15"))

USER_AGENT = os.getenv(
    "USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

MAX_ITEMS_PER_FEED = int(os.getenv("MAX_ITEMS_PER_FEED", "30"))
MAX_AGE_HOURS = int(os.getenv("MAX_AGE_HOURS", "48"))

# 배치 윈도우 기본 OFF
BATCH_WINDOW_SECONDS = int(os.getenv("BATCH_WINDOW_SECONDS", "0"))

# 한 통 최대 개수
MAX_ITEMS_PER_EMAIL = int(os.getenv("MAX_ITEMS_PER_EMAIL", "120"))

# 국가별 상한
MAX_US = int(os.getenv("MAX_US", "25"))
MAX_KR = int(os.getenv("MAX_KR", "25"))
MAX_JP = int(os.getenv("MAX_JP", "25"))

# JP 키워드 필터 (원하면 OFF)
JP_KEYWORD_MODE = os.getenv("JP_KEYWORD_MODE", "1").strip().lower() in ("1", "true", "yes")
JP_KEYWORDS = [k.strip() for k in os.getenv(
    "JP_KEYWORDS",
    "boj,bank of japan,yen,jpy,nikkei,tokyo stock,topix,fx,usd/jpy,semiconductor,hbm,chip,ai,robot,sony,toyota,softbank,tsmc,renesas,advantest,screen holdings,disco"
).split(",") if k.strip()]

JP_EXCLUDE_KEYWORDS = [k.strip() for k in os.getenv(
    "JP_EXCLUDE_KEYWORDS",
    "sports,baseball,soccer,entertainment,celebrity,crime"
).split(",") if k.strip()]

RESOLVE_FINAL_URL = os.getenv("RESOLVE_FINAL_URL", "0").strip().lower() in ("1", "true", "yes")

# -----------------------------
# 1) HTTP Session
# -----------------------------
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": USER_AGENT,
    "Accept": "application/rss+xml, application/xml;q=0.9, text/xml;q=0.8, */*;q=0.7",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7,ja;q=0.6",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
})

# -----------------------------
# 2) Feeds
# -----------------------------
FEEDS: List[Dict[str, str]] = [
    # US / Global
    {"category": "US", "name": "Reuters - Macro", "url": "https://www.reuters.com/rssFeed/macro"},
    {"category": "US", "name": "Reuters - World", "url": "https://www.reuters.com/world/rss"},
    {"category": "US", "name": "CNBC - Markets", "url": "https://www.cnbc.com/id/10000664/device/rss/rss.html"},
    {"category": "US", "name": "CNBC - Economy", "url": "https://www.cnbc.com/id/100003114/device/rss/rss.html"},

    # Korea
    {"category": "KR", "name": "YNA - Market", "url": "https://www.yna.co.kr/rss/market.xml"},
    {"category": "KR", "name": "YNA - Economy", "url": "https://www.yna.co.kr/rss/economy.xml"},

    # Japan
    {"category": "JP", "name": "NHK - Business", "url": "https://www3.nhk.or.jp/rss/news/cat5.xml"},
    {"category": "JP", "name": "Reuters - Japan Business", "url": "https://feeds.reuters.com/reuters/JPbusinessNews"},
    {"category": "JP", "name": "Nikkei - Top", "url": "https://www.nikkei.com/rss/news/cat0.xml"},
]

# -----------------------------
# 3) SMTP / Mail
# -----------------------------
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.daum.net")
SMTP_PORT = int(os.getenv("SMTP_PORT", "465"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASS = os.getenv("SMTP_PASS", "")

MAIL_TO   = os.getenv("MAIL_TO", SMTP_USER)
MAIL_FROM = os.getenv("MAIL_FROM", SMTP_USER)

SUBJECT_PREFIX = os.getenv("SUBJECT_PREFIX", "[RSS]")

CACHE_PATH = Path(os.getenv("CACHE_PATH", ".cache/rss/sent_cache.json"))
CACHE_MAX_KEYS = int(os.getenv("CACHE_MAX_KEYS", "5000"))

# -----------------------------
# 4) Helpers
# -----------------------------
def canonicalize_url(url: str) -> str:
    try:
        u = urlparse(url)
        qs = u.query
        if qs:
            kept = []
            for part in qs.split("&"):
                k = part.split("=", 1)[0].lower()
                if k.startswith("utm_") or k in ("ref", "fbclid", "gclid", "igshid"):
                    continue
                kept.append(part)
            qs = "&".join([p for p in kept if p])
        u2 = u._replace(query=qs, fragment="")
        return urlunparse(u2)
    except Exception:
        return url

def resolve_final_url(url: str) -> str:
    if not RESOLVE_FINAL_URL:
        return url
    try:
        r = SESSION.head(url, allow_redirects=True, timeout=REQUEST_TIMEOUT)
        if r.status_code >= 400 or not r.url:
            r = SESSION.get(url, allow_redirects=True, timeout=REQUEST_TIMEOUT)
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
                return datetime.fromtimestamp(time.mktime(entry[key]), tz=timezone.utc)
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

def make_key(category: str, feed_name: str, title: str, link: str) -> str:
    raw = f"{category}|{feed_name}|{title}|{link}".encode("utf-8", errors="ignore")
    return hashlib.sha256(raw).hexdigest()

def jp_keyword_pass(title: str) -> bool:
    if not JP_KEYWORD_MODE:
        return True
    t = (title or "").lower()
    if any(ex in t for ex in JP_EXCLUDE_KEYWORDS):
        return False
    return any(k.lower() in t for k in JP_KEYWORDS)

def filter_recent(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if MAX_AGE_HOURS <= 0:
        return items
    cutoff = datetime.now(timezone.utc).timestamp() - (MAX_AGE_HOURS * 3600)
    return [it for it in items if not it.get("time") or it["time"].timestamp() >= cutoff]

# -----------------------------
# 5) 제목 한글화(무료, 용어 치환)
# -----------------------------
def has_hangul(s: str) -> bool:
    return any('가' <= ch <= '힣' for ch in s)

_GLOSSARY = [
    (re.compile(r"\bBOJ\b", re.I), "일본 중앙은행(BOJ)"),
    (re.compile(r"\bBank of Japan\b", re.I), "일본 중앙은행(BOJ)"),
    (re.compile(r"\bNikkei\b", re.I), "니케이"),
    (re.compile(r"\bTOPIX\b", re.I), "TOPIX(도쿄 증시 지수)"),
    (re.compile(r"\bUSD/JPY\b", re.I), "달러/엔(USD/JPY)"),
    (re.compile(r"\bJPY\b", re.I), "엔화(JPY)"),
    (re.compile(r"\byen\b", re.I), "엔화"),
    (re.compile(r"\bFed\b", re.I), "미 연준(Fed)"),
    (re.compile(r"\bECB\b", re.I), "유럽중앙은행(ECB)"),
    (re.compile(r"\bCPI\b", re.I), "소비자물가(CPI)"),
    (re.compile(r"\bPPI\b", re.I), "생산자물가(PPI)"),
    (re.compile(r"\bGDP\b", re.I), "국내총생산(GDP)"),
]

def polish_ko_title(t: str) -> str:
    out = t
    for pat, rep in _GLOSSARY:
        out = pat.sub(rep, out)
    return re.sub(r"\s+", " ", out).strip()

def title_for_category(original: str, category: str) -> str:
    """
    KR: 그대로
    US/JP: 제목만 한글화(용어 치환)
    """
    t = (original or "").strip()
    if not t:
        return t

    if category == "KR":
        return t

    # 이미 한글이 있으면 다듬기만
    if has_hangul(t):
        return polish_ko_title(t)

    return polish_ko_title(t)

# -----------------------------
# 6) Fetch feed items
# -----------------------------
def fetch_feed_items(feed: Dict[str, str]) -> List[Dict[str, Any]]:
    try:
        content = SESSION.get(feed["url"], timeout=REQUEST_TIMEOUT).content
        parsed = feedparser.parse(content)
    except Exception:
        parsed = feedparser.parse(feed["url"])

    items: List[Dict[str, Any]] = []
    for entry in parsed.entries[:MAX_ITEMS_PER_FEED]:
        title = (entry.get("title") or "").strip()
        link  = (entry.get("link") or "").strip()
        if not title or not link:
            continue
        if feed["category"] == "JP" and not jp_keyword_pass(title):
            continue
        items.append({
            "category": feed["category"],
            "feed": feed["name"],
            "title": title,
            "link": canonicalize_url(link),
            "time": parse_entry_time(entry),
        })
    return items

# -----------------------------
# 7) Email HTML
# -----------------------------
CATEGORY_SUBJECT = {"US": "미국/글로벌", "KR": "한국", "JP": "일본"}

def escape_html(s: str) -> str:
    return (s.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")
              .replace('"',"&quot;").replace("'","&#39;"))

def build_email_html(items: List[Dict[str, Any]]) -> str:
    grouped: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
    for it in items:
        grouped.setdefault(it["category"], {}).setdefault(it["feed"], []).append(it)

    now_local = datetime.now().strftime("%Y-%m-%d %H:%M")
    html: List[str] = [
        f"<h2>{escape_html(SUBJECT_PREFIX)} {escape_html(now_local)}</h2>",
        "<p style='color:#666'>※ 미국/한국/일본 뉴스가 한 통으로 발송됩니다. (미국/일본은 <b>제목만</b> 한글화)</p>",
        "<hr/>",
    ]

    for category in ["US","KR","JP"]:
        feeds = grouped.get(category, {})
        if not feeds:
            continue
        html.append(f"<h2>[ {CATEGORY_SUBJECT.get(category, category)} ]</h2>")
        for feed_name, feed_items in feeds.items():
            html.append(f"<h3>{escape_html(feed_name)} ({len(feed_items)})</h3><ul>")
            for it in feed_items:
                shown_title = title_for_category(it["title"], it["category"])
                title = escape_html(shown_title)
                link = it["link"]
                if RESOLVE_FINAL_URL:
                    link = resolve_final_url(link)
                t = it.get("time")
                t_str = t.astimezone().strftime("%Y-%m-%d %H:%M") if t else ""
                meta = f" <small style='color:#666'>({escape_html(t_str)})</small>" if t_str else ""
                html.append(f"<li><a href='{escape_html(link)}'>{title}</a>{meta}</li>")
            html.append("</ul><br/>")
        html.append("<hr/>")
    return "\n".join(html)

# -----------------------------
# 8) SMTP Send
# -----------------------------
def send_mail(subject: str, html_body: str) -> None:
    if not SMTP_USER or not SMTP_PASS or not MAIL_TO:
        raise RuntimeError("SMTP_USER/SMTP_PASS/MAIL_TO 환경변수 비어있음")

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = MAIL_FROM
    msg["To"] = MAIL_TO
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    try:
        with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=30) as s:
            s.login(SMTP_USER, SMTP_PASS)
            s.sendmail(MAIL_FROM, [MAIL_TO], msg.as_string())
            return
    except Exception:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as s:
            s.ehlo(); s.starttls(); s.ehlo()
            s.login(SMTP_USER, SMTP_PASS)
            s.sendmail(MAIL_FROM, [MAIL_TO], msg.as_string())

# -----------------------------
# 9) Main
# -----------------------------
def main() -> int:
    cache = load_cache()

    all_items: List[Dict[str, Any]] = []
    for feed in FEEDS:
        try:
            all_items.extend(filter_recent(fetch_feed_items(feed)))
        except Exception:
            traceback.print_exc()

    fresh: List[Dict[str, Any]] = []
    now_ts = time.time()

    seen_links = set()
    for it in all_items:
        key = make_key(it["category"], it["feed"], it["title"], it["link"])
        link_key = canonicalize_url(it["link"])
        if key in cache:
            continue
        if link_key in seen_links:
            continue
        seen_links.add(link_key)

        cache[key] = now_ts
        fresh.append(it)

    if not fresh:
        print("[INFO] No new items.")
        save_cache(cache)
        return 0

    fresh.sort(key=lambda x: (x["time"].timestamp() if x["time"] else 0), reverse=True)

    if BATCH_WINDOW_SECONDS > 0:
        newest_ts = fresh[0]["time"].timestamp() if fresh[0].get("time") else now_ts
        cutoff = newest_ts - BATCH_WINDOW_SECONDS
        fresh = [it for it in fresh if (it.get("time").timestamp() if it.get("time") else newest_ts) >= cutoff]

    us = [x for x in fresh if x["category"] == "US"][:MAX_US]
    kr = [x for x in fresh if x["category"] == "KR"][:MAX_KR]
    jp = [x for x in fresh if x["category"] == "JP"][:MAX_JP]

    combined = us + kr + jp
    combined.sort(key=lambda x: (x["time"].timestamp() if x["time"] else 0), reverse=True)

    if MAX_ITEMS_PER_EMAIL > 0 and len(combined) > MAX_ITEMS_PER_EMAIL:
        combined = combined[:MAX_ITEMS_PER_EMAIL]

    subject = f"{SUBJECT_PREFIX} {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    send_mail(subject, build_email_html(combined))
    save_cache(cache)
    print(f"[OK] Sent {len(combined)} items to {MAIL_TO}")
    print(f"[INFO] Breakdown: US={len(us)} KR={len(kr)} JP={len(jp)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
