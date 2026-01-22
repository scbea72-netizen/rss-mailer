#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
rss_digest.py (RSS 뉴스 메일러 – 최종 안정판 / 시크릿 자동매핑)

정책
- KR: 제목 그대로
- US / JP: 제목만 한글 번역 (MyMemory 무료)
- 본문 번역 없음 (링크만)
- 번역 실패해도 메일은 무조건 발송

개선(중요)
- GitHub Secrets 이름이 SMTP_*가 아니어도 자동 인식:
  SMTP_* 우선 → HANMAIL_* → GMAIL_* 순으로 fallback
- SMTP 인증 실패(535)는 fallback으로 해결 안 되므로,
  원인 안내 메시지를 명확히 출력
- SSL(465) 우선, 네트워크 이슈일 때만 STARTTLS(587) 시도
"""

from __future__ import annotations

import os, json, time, hashlib, traceback
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

TITLE_TRANSLATE = os.getenv("TITLE_TRANSLATE", "1").strip() in ("1", "true", "yes", "on")
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
# 3. SMTP / Mail (AUTO-MAP)
# =====================
def _env_any(*keys: str, default: str = "") -> str:
    for k in keys:
        v = os.getenv(k)
        if v is not None and str(v).strip() != "":
            return str(v).strip()
    return default

# Host/Port: SMTP_HOST 우선, 없으면 HANMAIL용 기본값
SMTP_HOST = _env_any("SMTP_HOST", "HANMAIL_SMTP_HOST", default="smtp.daum.net")
SMTP_PORT = int(_env_any("SMTP_PORT", "HANMAIL_SMTP_PORT", default="465"))

# ✅ 계정/비번: SMTP_* 우선 → HANMAIL_* → GMAIL_* 순
SMTP_USER = _env_any("SMTP_USER", "HANMAIL_USER", "GMAIL_USER", default="")
SMTP_PASS = _env_any("SMTP_PASS", "HANMAIL_PASS", "GMAIL_APP_PASS", default="")

# ✅ 수신/발신: MAIL_* 우선 → HANMAIL_* → 기본 SMTP_USER
MAIL_TO   = _env_any("MAIL_TO", "HANMAIL_TO", default=SMTP_USER)
MAIL_FROM = _env_any("MAIL_FROM", "HANMAIL_FROM", default=SMTP_USER)

SUBJECT_PREFIX = _env_any("SUBJECT_PREFIX", default="[RSS]")

# =====================
# 4. CACHE
# =====================
CACHE_PATH = Path(".cache/rss/sent.json")
TITLE_CACHE_PATH = Path(".cache/rss/title.json")

def load_json(p: Path) -> Dict:
    if p.exists():
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def save_json(p: Path, d: Dict):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(d, ensure_ascii=False, indent=2), encoding="utf-8")

# =====================
# 5. UTIL
# =====================
def canonical(url: str) -> str:
    try:
        u = urlparse(url)
        qs = "&".join(
            p for p in (u.query or "").split("&") if p and not p.lower().startswith("utm_")
        )
        return urlunparse(u._replace(query=qs, fragment=""))
    except Exception:
        return url

def parse_time(e) -> Optional[datetime]:
    for k in ("published", "updated"):
        if getattr(e, "get", None) and e.get(k):
            try:
                d = dtparser.parse(e[k])
                return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
            except Exception:
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
    t = (text or "").strip()
    if not t:
        return t
    if has_ko(t):
        return t

    src = "ja" if looks_ja(t) else "en"
    key = f"{src}|{t}"
    if key in cache:
        return cache[key]

    out = t
    try:
        r = SESSION.get(
            "https://api.mymemory.translated.net/get",
            params={"q": t, "langpair": f"{src}|ko"},
            timeout=REQUEST_TIMEOUT
        )
        out = (r.json() or {}).get("responseData", {}).get("translatedText") or t
        out = str(out).strip() or t
    except Exception:
        out = t

    cache[key] = out
    if TRANSLATE_SLEEP_SECONDS > 0:
        time.sleep(TRANSLATE_SLEEP_SECONDS)
    return out

# =====================
# 7. FETCH
# =====================
def fetch(feed: Dict[str, str]) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    try:
        parsed = feedparser.parse(feed["url"], request_headers={"User-Agent": USER_AGENT})
        for e in parsed.entries[:MAX_ITEMS_PER_FEED]:
            title = (e.get("title") or "").strip()
            link = (e.get("link") or "").strip()
            if not title or not link:
                continue
            items.append({
                "category": feed["category"],
                "feed": feed["name"],
                "title": title,
                "link": canonical(link),
                "time": parse_time(e),
            })
    except Exception:
        traceback.print_exc()
    return items

# =====================
# 8. MAIL HTML
# =====================
def build_html(items: List[Dict[str, Any]], title_cache: Dict[str, str]) -> str:
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
# 9. SEND (ROBUST)
# =====================
def send(subject: str, html: str) -> None:
    if not SMTP_USER or not SMTP_PASS:
        raise RuntimeError(
            "SMTP 계정 정보가 비어있음. "
            "GitHub Secrets에 HANMAIL_USER/HANMAIL_PASS 또는 SMTP_USER/SMTP_PASS 또는 GMAIL_USER/GMAIL_APP_PASS를 설정하세요."
        )
    if not MAIL_TO:
        raise RuntimeError("수신자(MAIL_TO/HANMAIL_TO)가 비어있음")

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = MAIL_FROM or SMTP_USER
    msg["To"] = MAIL_TO
    msg.attach(MIMEText(html, "html", "utf-8"))

    # 1) SSL 우선
    try:
        with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=30) as s:
            s.login(SMTP_USER, SMTP_PASS)
            s.sendmail(msg["From"], [MAIL_TO], msg.as_string())
            return

    except smtplib.SMTPAuthenticationError as e:
        # ✅ 535는 보통 앱비밀번호/비번오류/SMTP허용 OFF
        raise RuntimeError(
            "SMTP 인증 실패(535). "
            "HANMAIL_PASS가 '앱 비밀번호'인지 확인하고, HANMAIL_USER가 전체 이메일 주소인지 확인하세요. "
            "또한 한메일 계정 보안설정에서 외부앱(SMTP) 허용/앱비밀번호 발급이 필요할 수 있습니다."
        ) from e

    except Exception:
        # 2) 네트워크/포트 이슈일 때만 STARTTLS fallback
        try:
            with smtplib.SMTP(SMTP_HOST, 587, timeout=30) as s:
                s.ehlo()
                s.starttls()
                s.ehlo()
                s.login(SMTP_USER, SMTP_PASS)
                s.sendmail(msg["From"], [MAIL_TO], msg.as_string())
                return
        except Exception as e2:
            raise RuntimeError(f"SMTP 전송 실패(SSL/STARTTLS 모두 실패): {e2}") from e2

# =====================
# 10. MAIN
# =====================
def main():
    sent = load_json(CACHE_PATH)
    title_cache = load_json(TITLE_CACHE_PATH)

    # 1) 수집
    items: List[Dict[str, Any]] = []
    for f in FEEDS:
        items.extend(fetch(f))

    # 2) 너무 오래된 뉴스 제거(옵션)
    if MAX_AGE_HOURS > 0:
        cutoff = datetime.now(timezone.utc).timestamp() - MAX_AGE_HOURS * 3600
        items = [it for it in items if not it["time"] or it["time"].timestamp() >= cutoff]

    # 3) 신규만 남기기
    fresh: List[Dict[str, Any]] = []
    for it in items:
        key = hashlib.sha1(f"{it['title']}{it['link']}".encode("utf-8", "ignore")).hexdigest()
        if key in sent:
            continue
        sent[key] = time.time()
        fresh.append(it)

    # 4) 최신순 정렬
    fresh.sort(key=lambda x: (x["time"].timestamp() if x["time"] else 0), reverse=True)

    # 5) 국가별 상한
    us = [x for x in fresh if x["category"] == "US"][:MAX_US]
    kr = [x for x in fresh if x["category"] == "KR"][:MAX_KR]
    jp = [x for x in fresh if x["category"] == "JP"][:MAX_JP]
    combined = us + kr + jp
    combined.sort(key=lambda x: (x["time"].timestamp() if x["time"] else 0), reverse=True)

    if MAX_ITEMS_PER_EMAIL > 0 and len(combined) > MAX_ITEMS_PER_EMAIL:
        combined = combined[:MAX_ITEMS_PER_EMAIL]

    if not combined:
        print("NO NEW ITEMS")
        save_json(CACHE_PATH, sent)
        save_json(TITLE_CACHE_PATH, title_cache)
        return

    html = build_html(combined, title_cache)
    subject = f"{SUBJECT_PREFIX} {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    send(subject, html)

    save_json(CACHE_PATH, sent)
    save_json(TITLE_CACHE_PATH, title_cache)

    print(f"SENT {len(combined)} ITEMS | US={len(us)} KR={len(kr)} JP={len(jp)}")
    print(f"SMTP_HOST={SMTP_HOST} PORT={SMTP_PORT} USER={(SMTP_USER[:3] + '***') if SMTP_USER else 'EMPTY'} TO={MAIL_TO}")

if __name__ == "__main__":
    main()
