import os, json, smtplib, requests, feedparser
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

RSS_URLS = [
    "https://feeds.reuters.com/reuters/businessNews",
    "https://feeds.reuters.com/reuters/worldNews"
]

STATE_FILE = "state.json"


def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"seen": []}


def save_state(st):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(st, f, ensure_ascii=False, indent=2)


def summarize(title):
    key = os.getenv("OPENAI_API_KEY", "")
    if not key:
        return ""
    r = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
        json={
            "model": "gpt-4.1-mini",
            "messages": [{"role": "user", "content": f"한줄 요약: {title}"}],
        },
    )
    return r.json()["choices"][0]["message"]["content"]


def main():
    st = load_state()
    seen = set(st["seen"])
    new = []

    for u in RSS_URLS:
        d = feedparser.parse(u)
        for e in d.entries[:20]:
            id = e.get("id") or e.get("link")
            if id not in seen:
                new.append((d.feed.title, e.title, e.link, id))

    if not new:
        return

    html = "<h2>오늘의 뉴스</h2>"
    for src, title, link, id in new[:30]:
        s = summarize(title)
        html += f"<p><b>{title}</b><br>{src}<br><a href='{link}'>링크</a><br>{s}</p>"
        seen.add(id)

    msg = MIMEMultipart()
    msg["Subject"] = f"[RSS] {len(new)}건"
    msg["From"] = os.environ["SMTP_USER"]
    msg["To"] = os.environ["MAIL_TO"]
    msg.attach(MIMEText(html, "html", "utf-8"))

    with smtplib.SMTP(os.environ["SMTP_HOST"], 587) as server:
        server.starttls()
        server.login(os.environ["SMTP_USER"], os.environ["SMTP_PASS"])
        server.send_message(msg)

    st["seen"] = list(seen)[-2000:]
    save_state(st)


main()
