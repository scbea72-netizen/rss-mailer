#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
rss_digest.py (all-in-one, overwrite-ready)

✅ What this version adds (based on your request):
1) "Send as soon as news appears" (practically):
   - The script already sends only when there are NEW items.
   - To make it near-real-time, run it more often (e.g., every 2 minutes).
     (GitHub Actions can do this via cron; see bottom notes.)
2) Japan news quality boost:
   - Stable JP feeds included
   - JP keyword focus (markets/FX/BOJ/semis/AI) + optional exclusions
   - Cross-feed de-duplication inside a single run
3) Better title translation for investing:
   - Title-only KO translation (keeps your rule)
   - Glossary post-processing (BOJ/yen/Nikkei etc.)

Dependencies:
  pip install requests feedparser python-dateutil googletrans==4.0.0rc1
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
from urllib.parse import urlparse, urlunparse

import requests
import feedparser
from dateutil import parser as dtparser
from googletrans import Translator


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

# "Send as soon as news appears" supporting knobs:
# - If you run this script frequently (cron */2 * * * *), this helps prevent spam bursts:
BATCH_WINDOW_SECONDS = int(os.getenv("BATCH_WINDOW_SECONDS", "90"))  # group items for ~1.5 minutes
MAX_ITEMS_PER_EMAIL = int(os.getenv("MAX_ITEMS_PER_EMAIL", "80"))

# JP focus
JP_KEYWORD_MODE = os.getenv("JP_KEYWORD_MODE", "1").strip().lower() in ("1", "true", "yes")
JP_KEYWORDS = [k.strip() for k in os.getenv(
    "JP_KEYWORDS",
    "boj,bank of japan,yen,jpy,nikkei,tokyo stock,topix,fx,usd/jpy,semiconductor,hbm,chip,ai,robot,sony,toyota,softbank,tsmc,renesas,advantest,screen holdings,disco"
).split(",") if k.strip()]

JP_EXCLUDE_KEYWORDS = [k.strip() for k in os.getenv(
    "JP_EXCLUDE_KEYWORDS",
    "sports,baseball,soccer,entertainment,celebrity,crime"
).split(",") if k.strip()]

# URL resolving (optional; slower)
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
# category: "US" | "KR" | "JP"
FEEDS: List[Dict[str, str]] = [
    # US / Global
    {"category": "US", "name": "Reuters - Macro", "url": "https://www.reuters.com/rssFeed/macro"},
    {"category": "US", "name": "Reuters - World", "url": "https://www.reuters.com/world/rss"},
    {"category": "US", "name": "CNBC - Markets", "url": "https://www.cnbc.com/id/10000664/device/rss/rss.html"},
    {"category": "US", "name": "CNBC - Economy", "url": "https://www.cnbc.com/id/100003114/device/rss/rss.html"},

    # Korea
    {"category": "KR", "name": "YNA - Market", "url": "https://www.yna.co.kr/rss/market.xml"},
    {"category": "KR", "name": "YNA - Economy", "url": "https://www.yna.co.kr/rss/economy.xml"},

    # Japan (stable)
    {"category": "JP", "name": "NHK - Business", "url": "https://www3.nhk.or.jp/rss/news/cat5.xml"},
    {"category": "JP", "name": "Reuters - Japan Business", "url": "https://feeds.reuters.com/reuters/JPbusinessNews"},
    {"category": "JP", "name": "Nikkei - Top", "url": "https://www.nikkei.com/rss/news/cat0.xml"},

    # Optional JP sources (may sometimes throttle, but kept as extra)
    {"category": "JP", "name": "The Japan Times - Top", "url": "https://www.japantimes.co.jp/feed/topstories/"},
    {"category": "JP", "name": "Nippon.com - News", "url": "https://www.nippon.com/en/news/feed/"},
]


# -----------------------------
# 3) SMTP / Mail
# -----------------------------
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.daum.net")
SMTP_PORT = int(os.getenv("SMTP_PORT", "465"))  # 465=SSL, 587=STARTTLS
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASS = os.getenv("SMTP_PASS", "")

MAIL_TO   = os.getenv("MAIL_TO", SMTP_USER)
MAIL_FROM = os.getenv("MAIL_FROM", SMTP_USER)

SUBJECT_PREFIX = os.getenv("SUBJECT_PREFIX", "[RSS]")

CACHE_PATH = Path(os.getenv("CACHE_PATH", ".cache/rss/sent_cache.json"))
CACHE_MAX_KEYS = int(os.getenv("CACHE_MAX_KEYS", "5000"))


# -----------------------------
# 4) Biztoc block
# -----------------------------
BIZTOC_HOST_RE = re.compile(r"(^|\.)biztoc\.com$", re.IGNORECASE)
JINA_PROXY_RE  = re.compile(r"^https?://r\.jina\.ai/https?://", re.IGNORECASE)

def is_biztoc_url(url: str) -> bool:
    try:
        u = urlparse(url)
        host = (u.hostname or "").lower()
        if BIZTOC_HOST_RE.search(host):
            return True
        if JINA_PROXY_RE.search(url) and "biztoc.com" in url.lower():
            return True
        return False
    except Exception:
        return False


# -----------------------------
# 5) Helpers
# -----------------------------
def canonicalize_url(url: str) -> str:
    """Drop tracking query params; keep stable identity for dedupe."""
    try:
        u = urlparse(url)
        # strip common tracking params
        qs = u.query
        if qs:
            # keep only "meaningful" params
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

def make_global_dedupe_key(title: str, link: str) -> str:
    """Cross-feed de-duplication within one run."""
    t = normalize_title(title)
    l = canonicalize_url(link)
    raw = f"{t}|{l}".encode("utf-8", errors="ignore")
    return hashlib.sha256(raw).hexdigest()

def fetch_feed_content(url: str) -> bytes:
    r = SESSION.get(url, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.content

def normalize_title(title: str) -> str:
    t = (title or "").strip().lower()
    # remove common source suffix patterns: " - Reuters", " | CNBC"
    t = re.sub(r"\s+[-|]\s+(reuters|cnbc|nhk|nikkei|the japan times|nippon\.com)\s*$", "", t, flags=re.I)
    # collapse whitespace, remove punctuation-ish
    t = re.sub(r"[\u200b\u200c\u200d]", "", t)
    t = re.sub(r"[\s]+", " ", t)
    t = re.sub(r"[\"'“”‘’]", "", t)
    return t[:180]

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
    out: List[Dict[str, Any]] = []
    for it in items:
        dt = it.get("time")
        if not dt or dt.timestamp() >= cutoff:
            out.append(it)
    return out


# -----------------------------
# 6) Title translation (KO only, with glossary polishing)
# -----------------------------
_TRANSLATOR: Optional[Translator] = None

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
]

def polish_ko_title(t: str) -> str:
    out = t
    for pat, rep in _GLOSSARY:
        out = pat.sub(rep, out)
    out = re.sub(r"\s+", " ", out).strip()
    return out

def translate_title_to_ko(title: str) -> str:
    global _TRANSLATOR
    title = (title or "").strip()
    if not title:
        return title
    if has_hangul(title):
        return title
    if len(title) < 6:
        return title

    try:
        if _TRANSLATOR is None:
            _TRANSLATOR = Translator()
        out = _TRANSLATOR.translate(title, src="auto", dest="ko")
        ko = (out.text or "").strip()
        return polish_ko_title(ko if ko else title)
    except Exception:
        return title


# -----------------------------
# 7) Fetch feed items
# -----------------------------
def fetch_feed_items(feed: Dict[str, str]) -> List[Dict[str, Any]]:
    url = feed["url"]
    if is_biztoc_url(url):
        return []

    try:
        content = fetch_feed_content(url)
        parsed = feedparser.parse(content)
    except Exception:
        parsed = feedparser.parse(url)

    items: List[Dict[str, Any]] = []
    for entry in parsed.entries[:MAX_ITEMS_PER_FEED]:
        title = (entry.get("title") or "").strip()
        link  = (entry.get("link") or "").strip()

        # alternative link fields
        for alt_key in ("feedburner_origlink", "origlink", "link"):
            alt = entry.get(alt_key)
            if isinstance(alt, str) and alt.strip():
                link = alt.strip()
                break

        if not title or not link:
            continue

        if is_biztoc_url(link):
            continue

        # JP keyword focus
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
# 8) Email HTML
# -----------------------------
CATEGORY_SUBJECT = {"US": "미국/글로벌", "KR": "한국", "JP": "일본"}

def escape_html(s: str) -> str:
    return (
        s.replace("&", "&amp;")
         .replace("<", "&lt;")
         .replace(">", "&gt;")
         .replace('"', "&quot;")
         .replace("'", "&#39;")
    )

def build_email_html(items: List[Dict[str, Any]]) -> str:
    grouped: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
    for it in items:
        grouped.setdefault(it["category"], {}).setdefault(it["feed"], []).append(it)

    now_local = datetime.now().strftime("%Y-%m-%d %H:%M")
    html: List[str] = [
        f"<h2>{escape_html(SUBJECT_PREFIX)} {escape_html(now_local)}</h2>",
        "<p style='color:#666'>※ 미국/한국/일본 뉴스가 한 통으로 발송됩니다. <b>제목만</b> 한국어로 번역됩니다.</p>",
        "<hr/>",
    ]

    for category in ["US", "KR", "JP"]:
        feeds = grouped.get(category, {})
        if not feeds:
            continue

        cat_name = CATEGORY_SUBJECT.get(category, category)
        html.append(f"<h2>[ {escape_html(cat_name)} ]</h2>")

        for feed_name in sorted(feeds.keys()):
            feed_items = feeds[feed_name]
            feed_items.sort(key=lambda x: (x["time"].timestamp() if x["time"] else 0), reverse=True)

            html.append(f"<h3>{escape_html(feed_name)} ({len(feed_items)})</h3>")
            html.append("<ul>")

            for it in feed_items:
                title_ko = translate_title_to_ko(it["title"])
                title = escape_html(title_ko)

                link = it["link"]
                if RESOLVE_FINAL_URL:
                    link = resolve_final_url(link)

                t = it.get("time")
                t_str = ""
                if t:
                    try:
                        t_str = t.astimezone().strftime("%Y-%m-%d %H:%M")
                    except Exception:
                        t_str = ""

                meta = f" <small style='color:#666'>({escape_html(t_str)})</small>" if t_str else ""
                html.append(f"<li><a href='{escape_html(link)}'>{title}</a>{meta}</li>")

            html.append("</ul><br/>")

        html.append("<hr/>")

    return "\n".join(html)


# -----------------------------
# 9) SMTP Send (SSL -> STARTTLS fallback)
# -----------------------------
def send_mail(subject: str, html_body: str) -> None:
    if not SMTP_USER or not SMTP_PASS or not MAIL_TO:
        raise RuntimeError("SMTP_USER/SMTP_PASS/MAIL_TO 환경변수가 비어있습니다. Secrets를 확인하세요.")

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = MAIL_FROM
    msg["To"] = MAIL_TO
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    last_err: Optional[Exception] = None

    # SSL first
    try:
        with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=30) as s:
            s.login(SMTP_USER, SMTP_PASS)
            s.sendmail(MAIL_FROM, [MAIL_TO], msg.as_string())
        return
    except Exception as e:
        last_err = e

    # STARTTLS fallback
    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as s:
            s.ehlo()
            s.starttls()
            s.ehlo()
            s.login(SMTP_USER, SMTP_PASS)
            s.sendmail(MAIL_FROM, [MAIL_TO], msg.as_string())
        return
    except Exception as e:
        last_err = e

    raise RuntimeError(f"SMTP 전송 실패: {last_err}")


# -----------------------------
# 10) Main
# -----------------------------
def main() -> int:
    cache = load_cache()

    # 1) Fetch
    all_items: List[Dict[str, Any]] = []
    for feed in FEEDS:
        try:
            items = filter_recent(fetch_feed_items(feed))
            all_items.extend(items)
        except Exception:
            print(f"[WARN] feed failed: {feed.get('category')} | {feed.get('name')} ({feed.get('url')})")
            traceback.print_exc()

    # 2) Cache de-dupe (already sent) + Cross-feed de-dupe (this run)
    fresh: List[Dict[str, Any]] = []
    now_ts = time.time()
    seen_global: set[str] = set()

    for it in all_items:
        cache_key = make_key(it["category"], it["feed"], it["title"], it["link"])
        if cache_key in cache:
            continue

        global_key = make_global_dedupe_key(it["title"], it["link"])
        if global_key in seen_global:
            continue
        seen_global.add(global_key)

        cache[cache_key] = now_ts
        fresh.append(it)

    if not fresh:
        print("[INFO] No new items to send.")
        save_cache(cache)
        return 0

    # 3) Sort newest first
    fresh.sort(key=lambda x: (x["time"].timestamp() if x["time"] else 0), reverse=True)

    # 4) Batch window (avoid sending multiple emails within a minute if you run every 1-2 min)
    #    If multiple runs happen quickly, this keeps mail calmer.
    if BATCH_WINDOW_SECONDS > 0:
        # Keep only the most recent window in this send; older ones will be sent next run if still fresh+unsent
        newest_ts = fresh[0]["time"].timestamp() if fresh[0].get("time") else now_ts
        cutoff = newest_ts - BATCH_WINDOW_SECONDS
        windowed = []
        for it in fresh:
            ts = it["time"].timestamp() if it.get("time") else newest_ts
            if ts >= cutoff:
                windowed.append(it)
        fresh = windowed

    # 5) Cap items per email
    if MAX_ITEMS_PER_EMAIL > 0 and len(fresh) > MAX_ITEMS_PER_EMAIL:
        fresh = fresh[:MAX_ITEMS_PER_EMAIL]

    subject = f"{SUBJECT_PREFIX} {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    html = build_email_html(fresh)
    send_mail(subject, html)

    save_cache(cache)
    print(f"[OK] Sent {len(fresh)} items to {MAIL_TO} | cache={CACHE_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""
🔧 GitHub Actions cron을 '거의 실시간'으로 바꾸는 추천값

- 2분 간격(권장, 안정적)
  cron: "*/2 * * * *"

- 1분 간격(더 빠름, 하지만 GitHub Actions가 종종 지연될 수 있음)
  cron: "* * * * *"

※ 이 스크립트는 "새 뉴스가 없으면 메일을 보내지 않기" 때문에
   자주 실행해도 스팸처럼 메일이 늘어나지 않습니다.
"""
