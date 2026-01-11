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
