import os, ssl, smtplib

host = os.environ["SMTP_HOST"]
port = int(os.environ.get("SMTP_PORT", 465))
user = os.environ["SMTP_USER"]
pwd  = os.environ["SMTP_PASS"]

ctx = ssl.create_default_context()

with smtplib.SMTP_SSL(host, port, context=ctx, timeout=30) as server:
    server.login(user, pwd)
    server.send_message(msg)

