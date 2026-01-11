import os, ssl, smtplib
from email.message import EmailMessage   # ← 여기로 올려야 함

host = os.environ["SMTP_HOST"]
port = int(os.environ.get("SMTP_PORT", 465))
user = os.environ["SMTP_USER"]
pwd  = os.environ["SMTP_PASS"]
mail_to = os.environ.get("MAIL_TO", user)

# 🔥 테스트용 body (RSS 완성되면 이 자리에 RSS digest 넣으면 됨)
body = "<h3>RSS Digest</h3><p>SMTP connection OK</p>"

msg = EmailMessage()
msg["Subject"] = "RSS Digest"
msg["From"] = user
msg["To"] = mail_to
msg.set_content(body, subtype="html")

ctx = ssl.create_default_context()
with smtplib.SMTP_SSL(host, port, context=ctx, timeout=30) as server:
    server.login(user, pwd)
    server.send_message(msg)
