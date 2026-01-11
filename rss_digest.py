import os, json
from datetime import timezone, timedelta
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import feedparser
import requests
from bs4 import BeautifulSoup

KST = timezone(timedelta(hours=9))

STATE_FILE = "state.json"

# ✅ 진짜 RSS 피드들 (중복 제거/정리)
RSS_URLS = [
    "https://www.coingecko.com/en/coins/nxt/rss",
    "https://feeds.feedburner.com/reuters/businessNews",
    "https://www.cnbc.com/id/10000664/device/rss/rss.html",
    "https://www.cnbc.com/id/10000618/device/rss/rss.html",
    "https://www.cnbc.com/id/100003114/device/rss/rss.html",
    "https://www.bea.gov/rss/rss.xml",
    "https://www.yna.co.kr/rss/market.xml",
    "https://www.yna.co.kr/rss/economy.xml",
    "https://opendart.fss.or.kr/api/rss.xml",
    "http://rss.hankooki.com/economy/sk_industry.xml",
    "https://www.hankyung.com/feed/finance",
    "https://www.korea.net/koreanet/rss/news/2",
    "https://www.korea.net/koreanet/rss/resources/79",
    "https://www.korea.net/koreanet/rss/government/all/104,105,106,143,109",
]

# ⚠️ RSS가 아닌 “HTML 페이지” (네이버 상승률) → 전용 파서로 처리
NAVER_RISE_PAGES = [
    ("Naver 코스피 상승률 TOP", "https://finance.naver.com/sise/sise_rise.nhn?sosok=0"),
    ("Naver 코스닥 상승률 TOP", "https://finance.naver.com/sise/sise_rise.nhn?sosok=1"),
]

def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"seen": []}

def save_state(s):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(s, f, ensure_ascii=False, indent=2)

def summarize(title: str) -> str:
    """
    OpenAI API Key가 있으면 1줄 요약을 생성하고,
    없으면 빈 문자열 반환.
    """
    key = os.getenv("OPENAI_API_KEY", "")
    if not key:
        return ""

    try:
        r = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
            json={
                "model": "gpt-4.1-mini",
                "messages": [{"role": "user", "content": f"다음 제목을 한국어로 한 줄 요약해줘: {title}"}],
            },
            timeout=30,
        )
        r.raise_for_status()
        return r.json()["choices"][0]["message"]["content"].strip()
    except Exception:
        return ""

def fetch_naver_rise(source_name: str, url: str, limit: int = 30):
    """
    네이버 상승률(HTML)을 파싱해서 RSS 엔트리처럼 변환
    """
    headers = {
        "User-Agent": "Mozilla/5.0"
    }
    res = requests.get(url, headers=headers, timeout=30)
    res.raise_for_status()

    soup = BeautifulSoup(res.text, "html.parser")

    items = []
    # 네이버 상승률 테이블은 보통 class="type_2" 테이블에 있음
    table = soup.select_one("table.type_2")
    if not table:
        return items

    rows = table.select("tr")
    for tr in rows:
        a = tr.select_one("a.tltle")
        if not a:
            continue
        name = a.get_text(strip=True)
        href = a.get("href", "")
        if not href:
            continue
        link = "https://finance.naver.com" + href

        # 등락률(퍼센트) 찾기
        tds = tr.select("td")
        change_pct = ""
        for td in tds:
            txt = td.get_text(" ", strip=True)
            if "%" in txt:
                change_pct = txt
                break

        title = f"{name} ({change_pct})" if change_pct else name
        uid = f"naver:{href}"

        items.append((source_name, title, link, uid))
        if len(items) >= limit:
            break

    return items

def main():
    st = load_state()
    seen = set(st.get("seen", []))
    new = []

    # 1) RSS 피드 수집
    for u in RSS_URLS:
        d = feedparser.parse(u)
        feed_title = getattr(d.feed, "title", u)

        for e in d.entries[:50]:
            link = getattr(e, "link", None)
            if not link:
                continue
            uid = getattr(e, "id", None) or link
            title = getattr(e, "title", link)

            if uid in seen:
                continue

            new.append((feed_title, title, link, uid))

    # 2) 네이버 상승률(HTML) 수집
    for src, url in NAVER_RISE_PAGES:
        try:
            items = fetch_naver_rise(src, url, limit=30)
            for (feed_title, title, link, uid) in items:
                if uid in seen:
                    continue
                new.append((feed_title, title, link, uid))
        except Exception:
            # 네이버 파싱이 실패해도 전체 작업이 죽지 않게
            pass

    if not new:
        return

    # 너무 길어지면 상위 60개만 발송
    new = new[:60]

    html = "<h2>오늘의 뉴스</h2>"
    for src, title, link, uid in new:
        s = summarize(title)
        html += f"<p><b>{title}</b><br>{src}<br><a href='{link}'>링크</a><br>{s}</p>"
        seen.add(uid)

    msg = MIMEMultipart()
    msg["Subject"] = f"[RSS] {len(new)}건"
    msg["From"] = os.environ["SMTP_USER"]
    msg["To"] = os.environ["MAIL_TO"]
    msg.attach(MIMEText(html, "html", "utf-8"))

   SMTP_HOST = os.environ["SMTP_HOST"]
SMTP_PORT = int(os.environ.get("SMTP_PORT", "465"))
SMTP_USER = os.environ["SMTP_USER"]
SMTP_PASS = os.environ["SMTP_PASS"]

with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=30) as server:
    server.login(SMTP_USER, SMTP_PASS)
    server.send_message(msg)
    st["seen"] = list(seen)[-4000:]
    save_state(st)

if __name__ == "__main__":
    main()
