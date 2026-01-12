import os
import ssl
import json
import time
import hashlib
import smtplib
import re
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import requests
import feedparser
from bs4 import BeautifulSoup
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# =========================
# Config / State
# =========================
STATE_DIR = Path(".state_test")
STATE_FILE = STATE_DIR / "state.json"

# 급등 기준
KOSPI_ALERT_PCT = 8.0
KOSDAQ_ALERT_PCT = 8.0

# 폭증 기준
VOLUME_SPIKE_RATIO = 5.0
VALUE_SPIKE_RATIO = 5.0

# 최소 거래대금 필터
MIN_VALUE_ABS = 50000

# 쿨다운(초)
COOLDOWN_US_SEC = 1800
COOLDOWN_KR_SEC = 1800
COOLDOWN_CRYPTO_SEC = 900
COOLDOWN_HOLDINGS_SEC = 0
COOLDOWN_DART_SEC = 0
COOLDOWN_SPIKES_SEC = 0

HEADERS = {"User-Agent": "Mozilla/5.0 (rss-mailer; GitHub Actions)"}

# =========================
# Data Sources
# =========================
NAVER_KOSPI_RISE = "https://finance.naver.com/sise/sise_rise.nhn?sosok=0"
NAVER_KOSDAQ_RISE = "https://finance.naver.com/sise/sise_rise.nhn?sosok=1"

RSS_SOURCES = {
    "US_MARKET": [
        "https://feeds.feedburner.com/reuters/businessNews",
        "https://www.cnbc.com/id/10000664/device/rss/rss.html",
        "https://www.cnbc.com/id/10000618/device/rss/rss.html",
        "https://www.cnbc.com/id/100003114/device/rss/rss.html",
        "https://www.bea.gov/rss/rss.xml",
    ],
    "KOREA_ECON_POLICY": [
        "https://www.yna.co.kr/rss/economy.xml",
        "https://www.yna.co.kr/rss/market.xml",
        "https://www.korea.net/Others/Subscribe-to-Koreanet/RSS-Service",
        "http://rss.hankooki.com/economy/sk_industry.xml",
    ],
    "DART": [
        "https://opendart.fss.or.kr/api/rss.xml",
    ],
    "CRYPTO": [
        "https://www.coingecko.com/en/coins/nxt/rss",
    ],
}

# 보유 종목 키워드(뉴스 매칭)
HOLDING_KEYWORDS = [
    "삼성전자우", "삼성전자",
    "루닛", "피앤티", "PNT",
    "비트플래닛", "가온아이",
    "한미반도체",
    "NVIDIA", "엔비디아", "NVDA",
]

# 한국 정책/경제 중요 키워드(강조)
KOREA_POLICY_KEYWORDS = [
    "정책", "금리", "기준금리", "인하", "인상",
    "세제", "세금", "규제", "완화",
    "부동산", "대출", "가계대출", "DSR",
    "환율", "원달러", "수출", "물가", "CPI",
]

# =========================
# Biztoc resolver
# - 기본 링크는 "안정뷰"로 강제( Biztoc 되돌아감 차단 )
# - 원문 추출되면 "원문" 버튼으로 함께 제공
# =========================
_BAD_HOSTS = (
    "biztoc.com",
    "twitter.com", "x.com", "facebook.com", "t.me", "telegram.me",
    "reddit.com", "youtube.com", "youtu.be", "linkedin.com",
    "gist.ai", "gista.ai",
    "accounts.google.com",
)

_PREFER_KEYWORDS = (
    "/news", "/tech", "/world", "/business", "/markets", "/article", "/stories", "/story",
    "reuters", "cnbc", "bloomberg", "wsj", "ft.com", "nytimes", "washingtonpost",
    "nbcnews", "apnews", "theverge", "axios", "economist", "coindesk", "cointelegraph",
)

def _pick_best_source_url_from_text(text: str) -> Optional[str]:
    urls = re.findall(r"https?://[^\s\"'<>]+", text or "")
    clean: List[str] = []
    for u in urls:
        u = u.strip().rstrip(").,;\"'")
        try:
            host = u.split("/")[2].lower()
        except Exception:
            continue
        if any(b in host for b in _BAD_HOSTS):
            continue
        clean.append(u)

    for u in clean:
        lu = u.lower()
        if any(k in lu for k in _PREFER_KEYWORDS):
            return u

    return clean[0] if clean else None

def resolve_biztoc(url: str) -> Tuple[str, Optional[str]]:
    if not url or "biztoc.com" not in url:
        return url, None

    # ✅ 절대 튕기지 않는 기본 링크(Reader)
    safe_link = "https://r.jina.ai/" + url
    source_link: Optional[str] = None

    # 1) biztoc 직접 접근 시도(성공하면 원문 추출)
    try:
        r = requests.get(url, headers=HEADERS, timeout=15, allow_redirects=True)
        if r.status_code < 400:
            source_link = _pick_best_source_url_from_text(r.text)
    except Exception:
        pass

    # 2) 막히면 reader에서 원문 추출 시도
    if not source_link:
        try:
            rr = requests.get(safe_link, headers=HEADERS, timeout=20, allow_redirects=True)
            if rr.status_code < 400:
                source_link = _pick_best_source_url_from_text(rr.text)
        except Exception:
            pass

    return safe_link, source_link

# =========================
# State helpers
# =========================
def load_state() -> Dict:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    if not STATE_FILE.exists():
        return {
            "seen_items": {},
            "last_risers": {"KOSPI": {}, "KOSDAQ": {}},
            "last_metrics": {"KOSPI": {}, "KOSDAQ": {}},
            "last_sent": {},
        }
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {
            "seen_items": {},
            "last_risers": {"KOSPI": {}, "KOSDAQ": {}},
            "last_metrics": {"KOSPI": {}, "KOSDAQ": {}},
            "last_sent": {},
        }

def save_state(state: Dict) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")

# =========================
# Utils
# =========================
def stable_id(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()

def html_escape(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def parse_int(s: str) -> int:
    s = (s or "").replace(",", "").strip()
    if s == "" or s == "-":
        return 0
    out = "".join(ch for ch in s if ch.isdigit())
    return int(out) if out else 0

def now_epoch() -> int:
    return int(time.time())

def cooldown_ok(state: Dict, bucket: str, cooldown_sec: int) -> bool:
    if cooldown_sec <= 0:
        return True
    last = int(state.get("last_sent", {}).get(bucket, 0))
    return (now_epoch() - last) >= cooldown_sec

def mark_sent(state: Dict, bucket: str) -> None:
    state.setdefault("last_sent", {})
    state["last_sent"][bucket] = now_epoch()

# =========================
# Naver rise scrape
# =========================
def fetch_risers(url: str, top_n: int = 30) -> List[Dict]:
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    table = soup.select_one("table.type_2")
    if not table:
        return []

    results = []
    for tr in table.select("tr"):
        a = tr.select_one("a.tltle")
        if not a:
            continue

        tds = tr.select("td")
        if len(tds) < 7:
            continue

        name = a.get_text(strip=True)
        href = a.get("href", "")
        code = href.split("code=")[-1].strip() if "code=" in href else ""

        price = tds[1].get_text(strip=True)

        pct_text = tds[4].get_text(strip=True).replace("%", "").replace("+", "").replace(",", "").strip()
        try:
            pct = float(pct_text)
        except Exception:
            continue

        vol = parse_int(tds[5].get_text(strip=True)) if len(tds) > 5 else 0
        val = parse_int(tds[6].get_text(strip=True)) if len(tds) > 6 else 0

        link = f"https://finance.naver.com{href}"
        results.append({"code": code, "name": name, "pct": pct, "price": price, "vol": vol, "val": val, "link": link})
        if len(results) >= top_n:
            break

    return results

def detect_price_alerts_and_spikes(state: Dict) -> Tuple[List[Dict], List[Dict], List[Dict], List[Dict]]:
    alerts_kospi: List[Dict] = []
    alerts_kosdaq: List[Dict] = []
    spikes_kospi: List[Dict] = []
    spikes_kosdaq: List[Dict] = []

    last_kospi_pct = state.get("last_risers", {}).get("KOSPI", {})
    last_kosdaq_pct = state.get("last_risers", {}).get("KOSDAQ", {})
    last_kospi_m = state.get("last_metrics", {}).get("KOSPI", {})
    last_kosdaq_m = state.get("last_metrics", {}).get("KOSDAQ", {})

    kospi_now = fetch_risers(NAVER_KOSPI_RISE, top_n=30)
    kosdaq_now = fetch_risers(NAVER_KOSDAQ_RISE, top_n=30)

    new_last_kospi_pct, new_last_kosdaq_pct = {}, {}
    new_last_kospi_m, new_last_kosdaq_m = {}, {}

    for it in kospi_now:
        key = it["code"] or it["name"]
        new_last_kospi_pct[key] = it["pct"]
        new_last_kospi_m[key] = {"vol": it["vol"], "val": it["val"]}

        if it["pct"] >= KOSPI_ALERT_PCT:
            prev = float(last_kospi_pct.get(key, -999))
            if (key not in last_kospi_pct) or (it["pct"] - prev >= 0.5):
                alerts_kospi.append(it)

        prev_m = last_kospi_m.get(key, {"vol": 0, "val": 0})
        pv, pval = int(prev_m.get("vol", 0)), int(prev_m.get("val", 0))
        vol_ratio = (it["vol"] / pv) if pv > 0 else 0.0
        val_ratio = (it["val"] / pval) if pval > 0 else 0.0

        abs_ok = it["val"] >= int(MIN_VALUE_ABS)
        if abs_ok and ((pv > 0 and vol_ratio >= VOLUME_SPIKE_RATIO) or (pval > 0 and val_ratio >= VALUE_SPIKE_RATIO)):
            if it["pct"] >= 5.0:
                it2 = dict(it)
                it2["vol_ratio"] = vol_ratio
                it2["val_ratio"] = val_ratio
                spikes_kospi.append(it2)

    for it in kosdaq_now:
        key = it["code"] or it["name"]
        new_last_kosdaq_pct[key] = it["pct"]
        new_last_kosdaq_m[key] = {"vol": it["vol"], "val": it["val"]}

        if it["pct"] >= KOSDAQ_ALERT_PCT:
            prev = float(last_kosdaq_pct.get(key, -999))
            if (key not in last_kosdaq_pct) or (it["pct"] - prev >= 0.5):
                alerts_kosdaq.append(it)

        prev_m = last_kosdaq_m.get(key, {"vol": 0, "val": 0})
        pv, pval = int(prev_m.get("vol", 0)), int(prev_m.get("val", 0))
        vol_ratio = (it["vol"] / pv) if pv > 0 else 0.0
        val_ratio = (it["val"] / pval) if pval > 0 else 0.0

        abs_ok = it["val"] >= int(MIN_VALUE_ABS)
        if abs_ok and ((pv > 0 and vol_ratio >= VOLUME_SPIKE_RATIO) or (pval > 0 and val_ratio >= VALUE_SPIKE_RATIO)):
            if it["pct"] >= 1.0:
                it2 = dict(it)
                it2["vol_ratio"] = vol_ratio
                it2["val_ratio"] = val_ratio
                spikes_kosdaq.append(it2)

    state.setdefault("last_risers", {})
    state.setdefault("last_metrics", {})
    state["last_risers"]["KOSPI"] = new_last_kospi_pct
    state["last_risers"]["KOSDAQ"] = new_last_kosdaq_pct
    state["last_metrics"]["KOSPI"] = new_last_kospi_m
    state["last_metrics"]["KOSDAQ"] = new_last_kosdaq_m

    return alerts_kospi, alerts_kosdaq, spikes_kospi, spikes_kosdaq

# =========================
# RSS fetch (new-only)
# =========================
def fetch_rss_new_items(state: Dict, category: str, urls: List[str]) -> List[Dict]:
    seen_list = state.get("seen_items", {}).get(category, [])
    seen = set(seen_list)
    new_items: List[Dict] = []

    for u in urls:
        feed = feedparser.parse(u)
        for e in getattr(feed, "entries", []):
            title = (e.get("title") or "").strip()
            link = (e.get("link") or "").strip()
            summary = (e.get("summary") or e.get("description") or "").strip()

            safe_link = link
            source_link = None
            if "biztoc.com" in (link or ""):
                safe_link, source_link = resolve_biztoc(link)

            sid = stable_id(f"{category}|{title}|{safe_link}")
            if sid in seen:
                continue

            new_items.append({
                "title": title,
                "link": safe_link,          # ✅ 기본 클릭: 안정뷰(절대 튕김 없음)
                "source_link": source_link, # ✅ 원문(있으면 버튼)
                "summary": summary,
                "source": u
            })
            seen.add(sid)

    state.setdefault("seen_items", {})
    state["seen_items"][category] = list(seen)[-4000:]
    return new_items

def filter_holdings_news(items: List[Dict]) -> List[Dict]:
    out: List[Dict] = []
    for it in items:
        text = f'{it.get("title","")} {it.get("summary","")}'
        if any(k.lower() in text.lower() for k in HOLDING_KEYWORDS):
            out.append(it)
    return out

def mark_policy_priority(items: List[Dict]) -> List[Dict]:
    for it in items:
        text = f'{it.get("title","")} {it.get("summary","")}'
        it["priority"] = any(k in text for k in KOREA_POLICY_KEYWORDS)
    return items

# =========================
# HTML builders
# =========================
def build_html_cards(title: str, items: List[Dict], badge_fn=None, max_n: int = 30) -> str:
    cards = []
    for it in items[:max_n]:
        t = html_escape(it.get("title") or it.get("name") or "")
        link = it.get("link") or "#"
        source_link = it.get("source_link")
        badge = badge_fn(it) if badge_fn else ""

        btns = ""
        if source_link:
            btns = f"""
              <div class="btnrow">
                <a class="btn" href="{source_link}" target="_blank" rel="noopener noreferrer">원문</a>
                <a class="btn ghost" href="{link}" target="_blank" rel="noopener noreferrer">안정뷰</a>
              </div>
            """

        cards.append(f"""
        <div class="card">
          <div class="row">
            <div class="title"><a href="{link}" target="_blank" rel="noopener noreferrer">{t}</a></div>
            {badge or ""}
          </div>
          {btns}
        </div>
        """)
    if not cards:
        return ""
    return f"<h3>{html_escape(title)}</h3>" + "\n".join(cards)

def build_market_html(
    alerts_kospi: List[Dict],
    alerts_kosdaq: List[Dict],
    spikes_kospi: List[Dict],
    spikes_kosdaq: List[Dict],
    dart_items: List[Dict],
    us_items: List[Dict],
    crypto_items: List[Dict],
    korea_items: List[Dict],
) -> str:
    style = """
    <style>
      body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Arial, sans-serif; margin: 0; padding: 0; }
      .wrap { padding: 14px; }
      .hdr { font-size: 18px; font-weight: 800; margin: 6px 0 10px; }
      .sub { color: #666; font-size: 12px; margin-bottom: 12px; }
      .card { border: 1px solid #eaeaea; border-radius: 14px; padding: 12px; margin: 10px 0; box-shadow: 0 1px 0 rgba(0,0,0,0.03); }
      .row { display: flex; align-items: center; justify-content: space-between; gap: 10px; }
      .title { font-size: 14px; font-weight: 700; line-height: 1.35; }
      .title a { text-decoration: none; color: #111; }
      .pill { display: inline-block; padding: 4px 8px; border-radius: 999px; background: #f4f4f4; font-size: 12px; white-space: nowrap; }
      .pill.hot { background: #ffe9e9; }
      .pill.warn { background: #fff5d6; }
      .btnrow { margin-top: 10px; display: flex; gap: 8px; }
      .btn { display:inline-block; padding: 8px 10px; border-radius: 10px; border: 1px solid #111; color:#111; text-decoration:none; font-weight:700; font-size:12px; }
      .btn.ghost { border-color:#ddd; color:#555; }
    </style>
    """

    def badge_price(it):
        return f'<div class="pill hot">+{it["pct"]:.2f}%</div>'

    def badge_spike(it):
        vr = it.get("vol_ratio", 0.0)
        br = it.get("val_ratio", 0.0)
        return f'<div class="pill warn">폭증 V{vr:.1f}x / T{br:.1f}x</div>'

    parts = [style, '<div class="wrap">']
    parts.append('<div class="hdr">📡 수시 레이더 (시장/공시/뉴스/코인)</div>')
    parts.append(f'<div class="sub">생성: {time.strftime("%Y-%m-%d %H:%M:%S")}</div>')

    if alerts_kospi:
        parts.append(build_html_cards("📈 코스피 +8% 급등", alerts_kospi, badge_fn=badge_price, max_n=30))
    if alerts_kosdaq:
        parts.append(build_html_cards("🚀 코스닥 +8% 급등", alerts_kosdaq, badge_fn=badge_price, max_n=30))
    if spikes_kospi:
        parts.append(build_html_cards("📊 코스피 거래량/대금 폭증", spikes_kospi, badge_fn=badge_spike, max_n=30))
    if spikes_kosdaq:
        parts.append(build_html_cards("📊 코스닥 거래량/대금 폭증", spikes_kosdaq, badge_fn=badge_spike, max_n=30))
    if dart_items:
        parts.append(build_html_cards("📌 공시(DART) 신규", dart_items, max_n=30))
    if us_items:
        parts.append(build_html_cards("🇺🇸 미국 속보(신규)", us_items, max_n=30))
    if crypto_items:
        parts.append(build_html_cards("🪙 코인(신규)", crypto_items, max_n=30))
    if korea_items:
        def badge_kr(it):
            return '<div class="pill warn">정책</div>' if it.get("priority") else ""
        parts.append(build_html_cards("🇰🇷 한국 경제/정책(신규)", korea_items, badge_fn=badge_kr, max_n=40))

    parts.append("</div>")
    return "\n".join([p for p in parts if p])

def build_holdings_html(holdings_news: List[Dict]) -> str:
    style = """
    <style>
      body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Arial, sans-serif; margin: 0; padding: 0; }
      .wrap { padding: 14px; }
      .hdr { font-size: 18px; font-weight: 800; margin: 6px 0 10px; }
      .sub { color: #666; font-size: 12px; margin-bottom: 12px; }
      .card { border: 1px solid #eaeaea; border-radius: 14px; padding: 12px; margin: 10px 0; }
      .title { font-size: 14px; font-weight: 700; line-height: 1.35; }
      .title a { text-decoration: none; color: #111; }
      .btnrow { margin-top: 10px; display: flex; gap: 8px; }
      .btn { display:inline-block; padding: 8px 10px; border-radius: 10px; border: 1px solid #111; color:#111; text-decoration:none; font-weight:700; font-size:12px; }
      .btn.ghost { border-color:#ddd; color:#555; }
    </style>
    """
    parts = [style, '<div class="wrap">']
    parts.append('<div class="hdr">🎯 보유 종목 관련 뉴스 (즉시)</div>')
    parts.append(f'<div class="sub">생성: {time.strftime("%Y-%m-%d %H:%M:%S")}</div>')

    for it in holdings_news[:40]:
        t = html_escape(it.get("title", ""))
        link = it.get("link", "#")
        source_link = it.get("source_link")

        btns = ""
        if source_link:
            btns = f"""
              <div class="btnrow">
                <a class="btn" href="{source_link}" target="_blank" rel="noopener noreferrer">원문</a>
                <a class="btn ghost" href="{link}" target="_blank" rel="noopener noreferrer">안정뷰</a>
              </div>
            """

        parts.append(f"""
        <div class="card">
          <div class="title"><a href="{link}" target="_blank" rel="noopener noreferrer">{t}</a></div>
          {btns}
        </div>
        """)

    parts.append("</div>")
    return "\n".join(parts)

# =========================
# Email send (Hanmail/Daum)
# =========================
def send_email(subject: str, html_body: str) -> None:
    SMTP_HOST = "smtp.daum.net"
    SMTP_PORT = 465

    user = os.environ["SMTP_USER"]
    pwd = os.environ["SMTP_PASS"]
    mail_to = os.environ.get("MAIL_TO", user)

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = user
    msg["To"] = mail_to

    plain = "자동 뉴스 요약 메일입니다.\n(HTML이 보이지 않으면 웹버전을 확인해주세요)"
    msg.attach(MIMEText(plain, "plain", "utf-8"))
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    ctx = ssl.create_default_context()
    with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, context=ctx, timeout=30) as server:
        server.login(user, pwd)
        server.send_message(msg)

# =========================
# Main
# =========================
def main():
    state = load_state()

    alerts_kospi, alerts_kosdaq, spikes_kospi, spikes_kosdaq = detect_price_alerts_and_spikes(state)

    us_all = fetch_rss_new_items(state, "US_MARKET", RSS_SOURCES["US_MARKET"])
    korea_all = fetch_rss_new_items(state, "KOREA_ECON_POLICY", RSS_SOURCES["KOREA_ECON_POLICY"])
    dart_all = fetch_rss_new_items(state, "DART", RSS_SOURCES["DART"])
    crypto_all = fetch_rss_new_items(state, "CRYPTO", RSS_SOURCES["CRYPTO"])

    holdings_news = filter_holdings_news(us_all + korea_all)
    korea_marked = mark_policy_priority(korea_all)

    send_holdings = bool(holdings_news) and cooldown_ok(state, "HOLDINGS", COOLDOWN_HOLDINGS_SEC)
    send_us = bool(us_all) and cooldown_ok(state, "US", COOLDOWN_US_SEC)
    send_kr = bool(korea_all) and cooldown_ok(state, "KR", COOLDOWN_KR_SEC)
    send_crypto = bool(crypto_all) and cooldown_ok(state, "CRYPTO", COOLDOWN_CRYPTO_SEC)
    send_dart = bool(dart_all) and cooldown_ok(state, "DART", COOLDOWN_DART_SEC)
    send_spikes = bool(alerts_kospi or alerts_kosdaq or spikes_kospi or spikes_kosdaq) and cooldown_ok(state, "SPIKES", COOLDOWN_SPIKES_SEC)

    market_us = us_all if send_us else []
    market_kr = korea_marked if send_kr else []
    market_crypto = crypto_all if send_crypto else []
    market_dart = dart_all if send_dart else []
    market_alerts_kospi = alerts_kospi if send_spikes else []
    market_alerts_kosdaq = alerts_kosdaq if send_spikes else []
    market_spikes_kospi = spikes_kospi if send_spikes else []
    market_spikes_kosdaq = spikes_kosdaq if send_spikes else []

    save_state(state)

    if send_holdings:
        html_h = build_holdings_html(holdings_news)
        send_email("[보유종목 즉시] 뉴스", html_h)
        mark_sent(state, "HOLDINGS")

    has_market_any = any([
        market_alerts_kospi, market_alerts_kosdaq,
        market_spikes_kospi, market_spikes_kosdaq,
        market_dart, market_us, market_crypto, market_kr
    ])

    if has_market_any:
        tags = []
        if market_alerts_kospi or market_alerts_kosdaq:
            tags.append("급등")
        if market_spikes_kospi or market_spikes_kosdaq:
            tags.append("폭증")
        if market_dart:
            tags.append("공시")
        if market_us:
            tags.append("미국")
        if market_crypto:
            tags.append("코인")
        if market_kr:
            tags.append("한국경제")

        subject = f"[수시레이다] {'/'.join(tags)}"
        html_m = build_market_html(
            market_alerts_kospi, market_alerts_kosdaq,
            market_spikes_kospi, market_spikes_kosdaq,
            market_dart, market_us, market_crypto, market_kr
        )
        send_email(subject, html_m)

        if send_us: mark_sent(state, "US")
        if send_kr: mark_sent(state, "KR")
        if send_crypto: mark_sent(state, "CRYPTO")
        if send_dart: mark_sent(state, "DART")
        if send_spikes: mark_sent(state, "SPIKES")

    save_state(state)
    print("Done.")

if __name__ == "__main__":
    main()
