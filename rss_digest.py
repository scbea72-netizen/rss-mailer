#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
rss_digest.py
- RSS 새 글만 메일 발송 (중복 제거 캐시)
- Biztoc 차단
- 제목만 한국어로 번역(영어/일본어 등 비한글 제목 → 한국어)
- 미국/한국/일본(US/KR/JP) 3개 소스를 "한 통"의 메일로 합쳐서 발송
- SMTP: SSL/STARTTLS 자동 재시도 (Secrets 확인 불가 상황도 대응)

핵심 개선(일본 RSS 미수신 해결):
- feedparser가 URL을 직접 파싱할 때 차단/실패하는 경우가 많아서,
  requests로 먼저 가져오고(User-Agent 포함) 그 내용을 feedparser로 파싱하도록 변경
- JP 피드를 안정적인 소스로 보강 (NHK/Reuters JP/Nikkei 등)

Requirements:
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
from typing import List, Dict, Any, Optional

import requests
import feedparser
from dateutil import parser as dtparser
from googletrans import Translator


# -----------------------------
# 0) HTTP 기본 설정
# -----------------------------
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "15"))
USER_AGENT = os.getenv(
    "USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": USER_AGENT,
    "Accept": "application/rss+xml, application/xml;q=0.9, text/xml;q=0.8, */*;q=0.7",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7,ja;q=0.6",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
})


# -----------------------------
# 1) RSS FEEDS (일본 안정 소스 보강)
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
    {"category": "KR", "name": "DART - Disclosures", "url": "https://opendart.fss.or.kr/api/rss.xml"},

    # Japan (안정/대체 소스 위주)
    # NHK: 경제/비즈니스 성격이 섞인 뉴스 카테고리 RSS
    {"category": "JP", "name": "NHK - Business", "url": "https://www3.nhk.or.jp/rss/news/cat5.xml"},
    # Reuters Japan Business (지역/카테고리 피드가 동작하는 경우가 많음)
    {"category": "JP", "name": "Reuters - Japan Business", "url": "https://feeds.reuters.com/reuters/JPbusinessNews"},
    # Nikkei RSS (간혹 제한이 있을 수 있으나 RSS 자체는 꽤 안정적으로 동작)
    {"category": "JP", "name": "Nikkei - Top", "url": "https://www.nikkei.com/rss/news/cat0.xml"},

    # 기존 소스(가끔 차단/빈 피드가 되는 경우가 있어 유지하되, 위의 안정 소스가 메인)
    {"category": "JP", "name": "The Japan Times - Top Stories", "url": "https://www.japantimes.co.jp/feed/topstories/"},
    {"category": "JP", "name": "Nippon.com - News", "url": "https://www.nippon.com/en/news/feed/"},
    {"category": "JP", "name": "Digital Agency (Japan) - News", "url": "https://www.digital.go.jp/feed.xml"},
]


# -----------------------------
# 2) SMTP / MAIL SETTINGS
# -----------------------------
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.daum.net")
SMTP_PORT = int(os.getenv("SMTP_PORT", "465"))  # 465=SSL, 587=STARTTLS
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASS = os.getenv("SMTP_PASS", "")

MAIL_TO   = os.getenv("MAIL_TO", SMTP_USER)
MAIL_FROM = os.getenv("MAIL_FROM", SMTP_USER)

SUBJECT_PREFIX = os.getenv("SUBJECT_PREFIX", "[RSS]")
MAX_ITEMS_PER_FEED = int(os.getenv("MAX_ITEMS_PER_FEED", "30"))
MAX_AGE_HOURS = int(os.getenv("MAX_AGE_HOURS", "48"))

CACHE_PATH = Path(os.getenv("CACHE_PATH", ".cache/rss/sent_cache.json"))
CACHE_MAX_KEYS = int(os.getenv("CACHE_MAX_KEYS", "5000"))

# 1이면 링크 최종 도착지로 해석(리다이렉트 추적). 기본은 0(빠르고 안전)
RESOLVE_FINAL_URL = os.getenv("RESOLVE_FINAL_URL", "0").strip().lower() in ("1", "true", "yes")


# -----------------------------
# 3) BIZTOC BLOCK RULES
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


def fetch_feed_content(url: str) -> bytes:
    """
    일본 RSS가 안 들어오는 대표 원인:
    - feedparser.parse(url)이 내부적으로 User-Agent 없이 접근하거나,
      사이트가 봇 접근을 막아서 entries가 비어버림
    해결:
    - requests로 먼저 content를 받아 feedparser에 bytes로 넘김
    """
    r = SESSION.get(url, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.content


def fetch_feed_items(feed: Dict[str, str]) -> List[Dict[str, Any]]:
    url = feed["url"]
    if is_biztoc_url(url):
        return []

    # 1) content 먼저 가져오기(차단 회피/호환성 향상)
    try:
        content = fetch_feed_content(url)
        parsed = feedparser.parse(content)
    except Exception:
        # 2) 그래도 실패하면 feedparser URL 파싱으로 한 번 더(예비)
        parsed = feedparser.parse(url)

    items: List[Dict[str, Any]] = []

    for entry in parsed.entries[:MAX_ITEMS_PER_FEED]:
        title = (entry.get("title") or "").strip()
        link  = (entry.get("link") or "").strip()

        for alt_key in ("feedburner_origlink", "origlink", "link"):
            alt = entry.get(alt_key)
            if isinstance(alt, str) and alt.strip():
                link = alt.strip()
                break

        if not title or not link:
            continue

        if is_biztoc_url(link):
            continue

        items.append({
            "category": feed["category"],
            "feed": feed["name"],
            "title": title,
            "link": link,
            "time": parse_entry_time(entry),
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


# -----------------------------
# 4) 제목 한국어 번역 (일본어 포함)
# -----------------------------
_TRANSLATOR: Optional[Translator] = None

def has_hangul(s: str) -> bool:
    return any('가' <= ch <= '힣' for ch in s)

def translate_title_to_ko(title: str) -> str:
    global _TRANSLATOR
    title = (title or "").strip()
    if not title or has_hangul(title):
        return title

    # 너무 짧으면 번역 스킵
    if len(title) < 6:
        return title

    try:
        if _TRANSLATOR is None:
            _TRANSLATOR = Translator()
        out = _TRANSLATOR.translate(title, src="auto", dest="ko")
        ko = (out.text or "").strip()
        return ko if ko else title
    except Exception:
        return title


# -----------------------------
# 5) 메일 HTML 생성 (US/KR/JP 한 통)
# -----------------------------
CATEGORY_SUBJECT = {"US": "미국/글로벌", "KR": "한국", "JP": "일본"}

def build_email_html(items: List[Dict[str, Any]]) -> str:
    # category -> feed -> items
    grouped: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
    for it in items:
        grouped.setdefault(it["category"], {}).setdefault(it["feed"], []).append(it)

    now_local = datetime.now().strftime("%Y-%m-%d %H:%M")
    html: List[str] = [
        f"<h2>{SUBJECT_PREFIX} {now_local}</h2>",
        "<p style='color:#666'>※ 미국/한국/일본 뉴스가 한 통으로 발송됩니다. 제목만 한국어로 번역됩니다.</p>",
        "<hr/>",
    ]

    # 항상 US -> KR -> JP 순서
    for category in ["US", "KR", "JP"]:
        feeds = grouped.get(category, {})
        if not feeds:
            continue

        cat_name = CATEGORY_SUBJECT.get(category, category)
        html.append(f"<h2>[ {escape_html(cat_name)} ]</h2>")

        # feed 이름 정렬 (보기 좋게)
        for feed_name in sorted(feeds.keys()):
            feed_items = feeds[feed_name]
            feed_items.sort(key=lambda x: (x["time"].timestamp() if x["time"] else 0), reverse=True)

            html.append(f"<h3>{escape_html(feed_name)} ({len(feed_items)})</h3>")
            html.append("<ul>")

            for it in feed_items:
                title_ko = translate_title_to_ko(it["title"])
                title = escape_html(title_ko)

                final_link = resolve_final_url(it["link"])

                t = it.get("time")
                t_str = ""
                if t:
                    try:
                        t_str = t.astimezone().strftime("%Y-%m-%d %H:%M")
                    except Exception:
                        t_str = ""

                meta = f" <small style='color:#666'>({t_str})</small>" if t_str else ""
                html.append(f"<li><a href='{final_link}'>{title}</a>{meta}</li>")

            html.append("</ul><br/>")

        html.append("<hr/>")

    return "\n".join(html)


# -----------------------------
# 6) SMTP 발송 (SSL → STARTTLS 자동 재시도)
# -----------------------------
def send_mail(subject: str, html_body: str) -> None:
    if not SMTP_USER or not SMTP_PASS or not MAIL_TO:
        raise RuntimeError("SMTP_USER/SMTP_PASS/MAIL_TO 환경변수가 비어있습니다. GitHub Secrets를 확인하세요.")

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = MAIL_FROM
    msg["To"] = MAIL_TO
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    last_err: Optional[Exception] = None

    # 1) SSL 우선
    try:
        with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=30) as s:
            s.login(SMTP_USER, SMTP_PASS)
            s.sendmail(MAIL_FROM, [MAIL_TO], msg.as_string())
        return
    except Exception as e:
        last_err = e

    # 2) STARTTLS 재시도
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
# 7) MAIN
# -----------------------------
def main() -> int:
    cache = load_cache()
    all_items: List[Dict[str, Any]] = []

    for feed in FEEDS:
        try:
            items = filter_recent(fetch_feed_items(feed))
            all_items.extend(items)
        except Exception:
            print(f"[WARN] feed failed: {feed.get('category')} | {feed.get('name')} ({feed.get('url')})")
            traceback.print_exc()

    fresh: List[Dict[str, Any]] = []
    now_ts = time.time()

    for it in all_items:
        key = make_key(it["category"], it["feed"], it["title"], it["link"])
        if key in cache:
            continue
        cache[key] = now_ts
        fresh.append(it)

    if not fresh:
        print("[INFO] No new items to send.")
        save_cache(cache)
        return 0

    # 전체를 한 통으로 발송
    fresh.sort(key=lambda x: (x["time"].timestamp() if x["time"] else 0), reverse=True)
    subject = f"{SUBJECT_PREFIX} {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    html = build_email_html(fresh)
    send_mail(subject, html)

    save_cache(cache)
    print(f"[OK] Sent {len(fresh)} items to {MAIL_TO} | cache={CACHE_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

