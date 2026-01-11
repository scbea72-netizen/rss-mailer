import os
import ssl
import json
import time
import hashlib
import smtplib
from pathlib import Path
from typing import Dict, List, Tuple

import requests
import feedparser
from bs4 import BeautifulSoup
from email.message import EmailMessage

STATE_DIR = Path(".state")
STATE_FILE = STATE_DIR / "state.json"

# =========================
# 승찬님 급등 "긴급" 기준: +8%
# =========================
KOSPI_ALERT_PCT = 8.0
KOSDAQ_ALERT_PCT = 8.0

# 네이버 상승(급등) 페이지
NAVER_KOSPI_RISE = "https://finance.naver.com/sise/sise_rise.nhn?sosok=0"
NAVER_KOSDAQ_RISE = "https://finance.naver.com/sise/sise_rise.nhn?sosok=1"

# RSS 소스(필요 시 추가 가능)
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

# 한국 경제/정책 중요 키워드(표시용)
KOREA_POLICY_KEYWORDS = [
    "정책", "금리", "기준금리", "인하", "인상",
    "세제", "세금", "규제", "완화",
    "부동산", "대출", "가계대출", "DSR",
    "환율", "원달러", "수출", "물가", "CPI",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (rss-mailer; GitHub Actions)",
}


def load_state() -> Dict:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    if not STATE_FILE.exists():
        return {
            "seen_items": {},  # category -> list of ids
            "last_risers": {"KOSPI": {}, "KOSDAQ": {}},  # code/name -> pct
        }
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {
            "seen_items": {},
            "last_risers": {"KOSPI": {}, "KOSDAQ": {}},
        }


def save_state(state: Dict) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def stable_id(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()


def html_escape(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def fetch_risers(url: str, top_n: int = 30) -> List[Dict]:
    """
    네이버 상승 페이지에서 TOP N 종목 추출
    반환: [{code, name, pct, price, link}]
    """
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
        if len(tds) < 5:
            continue

        name = a.get_text(strip=True)
        href = a.get("href", "")
        code = ""
        if "code=" in href:
            code = href.split("code=")[-1].strip()

        price = tds[1].get_text(strip=True)

        pct_text = tds[4].get_text(strip=True)
        pct_text = pct_text.replace("%", "").replace("+", "").replace(",", "").strip()
        try:
            pct = float(pct_text)
        except Exception:
            continue

        link = f"https://finance.naver.com{href}"

        results.append(
            {"code": code, "name": name, "pct": pct, "price": price, "link": link}
        )
        if len(results) >= top_n:
            break

    return results


def detect_riser_alerts(state: Dict) -> Tuple[List[Dict], List[Dict]]:
    """
    코스피/코스닥 급등 알림 감지.
    - pct >= 임계치(승찬님: +8%)
    - 알림 조건: 신규 등장 OR (이전 대비 +0.5%p 이상 상승)
    """
    alerts_kospi: List[Dict] = []
    alerts_kosdaq: List[Dict] = []

    last_kospi = state.get("last_risers", {}).get("KOSPI", {})
    last_kosdaq = state.get("last_risers", {}).get("KOSDAQ", {})

    kospi_now = fetch_risers(NAVER_KOSPI_RISE, top_n=30)
    kosdaq_now = fetch_risers(NAVER_KOSDAQ_RISE, top_n=30)

    new_last_kospi = {}
    for it in kospi_now:
        key = it["code"] or it["name"]
        new_last_kospi[key] = it["pct"]
        if it["pct"] >= KOSPI_ALERT_PCT:
            prev = float(last_kospi.get(key, -999))
            if (key not in last_kospi) or (it["pct"] - prev >= 0.5):
                alerts_kospi.append(it)

    new_last_kosdaq = {}
    for it in kosdaq_now:
        key = it["code"] or it["name"]
        new_last_kosdaq[key] = it["pct"]
        if it["pct"] >= KOSDAQ_ALERT_PCT:
            prev = float(last_kosdaq.get(key, -999))
            if (key not in last_kosdaq) or (it["pct"] - prev >= 0.5):
                alerts_kosdaq.append(it)

    state.setdefault("last_risers", {})
    state["last_risers"]["KOSPI"] = new_last_kospi
    state["last_risers"]["KOSDAQ"] = new_last_kosdaq

    return alerts_kospi, alerts_kosdaq


def fetch_rss_new_items(state: Dict, category: str, urls: List[str]) -> List[Dict]:
    """
    RSS에서 신규 아이템만 반환(중복 방지)
    """
    seen_list = state.get("seen_items", {}).get(category, [])
    seen = set(seen_list)
    new_items: List[Dict] = []

    for u in urls:
        feed = feedparser.parse(u)
        for e in getattr(feed, "entries", []):
            title = (e.get("title") or "").strip()
            link = (e.get("link") or "").strip()
            summary = (e.get("summary") or e.get("description") or "").strip()

            sid = stable_id(f"{category}|{title}|{link}")
            if sid in seen:
                continue

            new_items.append(
                {
                    "title": title,
                    "link": link,
                    "summary": summary,
                    "source": u,
                }
            )
            seen.add(sid)

    # 너무 커지지 않게 제한
    state.setdefault("seen_items", {})
    state["seen_items"][category] = list(seen)[-2000:]
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


def build_html(
    risers_kospi: List[Dict],
    risers_kosdaq: List[Dict],
    holdings_news: List[Dict],
    dart_items: List[Dict],
    us_items: List[Dict],
    crypto_items: List[Dict],
    korea_items: List[Dict],
) -> str:
    parts: List[str] = []
    parts.append("<h2>📬 수시 알림 Digest</h2>")
    parts.append(f"<p>생성 시각: {time.strftime('%Y-%m-%d %H:%M:%S')}</p>")

    def section(title: str, body: str):
        parts.append("<hr>")
        parts.append(f"<h3>{title}</h3>")
        parts.append(body)

    # 1) 급등
    if risers_kospi or risers_kosdaq:
        rows: List[str] = []
        if risers_kospi:
            rows.append("<h4>📈 코스피 급등(알림)</h4><ul>")
            for it in risers_kospi:
                rows.append(
                    f'<li><a href="{it["link"]}">{html_escape(it["name"])}</a> '
                    f'- {html_escape(it["price"])} / <b>+{it["pct"]:.2f}%</b></li>'
                )
            rows.append("</ul>")
        if risers_kosdaq:
            rows.append("<h4>🚀 코스닥 급등(알림)</h4><ul>")
            for it in risers_kosdaq:
                rows.append(
                    f'<li><a href="{it["link"]}">{html_escape(it["name"])}</a> '
                    f'- {html_escape(it["price"])} / <b>+{it["pct"]:.2f}%</b></li>'
                )
            rows.append("</ul>")
        section("1) 급등 종목", "\n".join(rows))

    # 2) 보유 종목 뉴스
    if holdings_news:
        rows = ["<ul>"]
        for it in holdings_news[:30]:
            rows.append(f'<li><a href="{it["link"]}">{html_escape(it["title"])}</a></li>')
        rows.append("</ul>")
        section("2) 보유 종목 관련 뉴스(신규)", "\n".join(rows))

    # 3) 공시
    if dart_items:
        rows = ["<ul>"]
        for it in dart_items[:30]:
            rows.append(f'<li><a href="{it["link"]}">{html_escape(it["title"])}</a></li>')
        rows.append("</ul>")
        section("3) 공시(DART) 신규", "\n".join(rows))

    # 4) 미국 속보
    if us_items:
        rows = ["<ul>"]
        for it in us_items[:30]:
            rows.append(f'<li><a href="{it["link"]}">{html_escape(it["title"])}</a></li>')
        rows.append("</ul>")
        section("4) 미국 증시/거시 속보(신규)", "\n".join(rows))

    # 5) 코인
    if crypto_items:
        rows = ["<ul>"]
        for it in crypto_items[:30]:
            rows.append(f'<li><a href="{it["link"]}">{html_escape(it["title"])}</a></li>')
        rows.append("</ul>")
        section("5) 코인(신규)", "\n".join(rows))

    # 7) 한국 경제/정책
    if korea_items:
        rows = ["<ul>"]
        for it in korea_items[:40]:
            prefix = "🟠 " if it.get("priority") else ""
            rows.append(f'<li>{prefix}<a href="{it["link"]}">{html_escape(it["title"])}</a></li>')
        rows.append("</ul>")
        section("7) 한국 경제/정책(신규)", "\n".join(rows))

    return "\n".join(parts)


def send_email(subject: str, html_body: str) -> None:
    host = os.environ["SMTP_HOST"]
    port = int(os.environ.get("SMTP_PORT", 465))
    user = os.environ["SMTP_USER"]
    pwd = os.environ["SMTP_PASS"]
    mail_to = os.environ.get("MAIL_TO", user)

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = user
    msg["To"] = mail_to
    msg.set_content(html_body, subtype="html")

    ctx = ssl.create_default_context()
    with smtplib.SMTP_SSL(host, port, context=ctx, timeout=30) as server:
        server.login(user, pwd)
        server.send_message(msg)


def main():
    state = load_state()

    # 1) 급등 감지
    risers_kospi, risers_kosdaq = detect_riser_alerts(state)

    # 2~7) RSS 신규 수집
    us_all = fetch_rss_new_items(state, "US_MARKET", RSS_SOURCES["US_MARKET"])
    korea_all = fetch_rss_new_items(state, "KOREA_ECON_POLICY", RSS_SOURCES["KOREA_ECON_POLICY"])
    dart_all = fetch_rss_new_items(state, "DART", RSS_SOURCES["DART"])
    crypto_all = fetch_rss_new_items(state, "CRYPTO", RSS_SOURCES["CRYPTO"])

    holdings_news = filter_holdings_news(us_all + korea_all)
    korea_marked = mark_policy_priority(korea_all)

    # 새 소식이 하나도 없으면 메일 안 보냄
    has_any = any(
        [
            risers_kospi,
            risers_kosdaq,
            holdings_news,
            dart_all,
            us_all,
            crypto_all,
            korea_all,
        ]
    )

    save_state(state)

    if not has_any:
        print("No new items. Skip sending email.")
        return

    # 메일 제목 태그
    subject_tags: List[str] = []
    if risers_kospi or risers_kosdaq:
        subject_tags.append("급등")
    if holdings_news:
        subject_tags.append("보유뉴스")
    if dart_all:
        subject_tags.append("공시")
    if us_all:
        subject_tags.append("미국")
    if crypto_all:
        subject_tags.append("코인")
    if korea_all:
        subject_tags.append("한국경제")

    subject = f"[수시알림] {'/'.join(subject_tags)}"
    html_body = build_html(
        risers_kospi,
        risers_kosdaq,
        holdings_news,
        dart_all,
        us_all,
        crypto_all,
        korea_marked,
    )

    send_email(subject, html_body)
    print("Email sent.")


if __name__ == "__main__":
    main()
