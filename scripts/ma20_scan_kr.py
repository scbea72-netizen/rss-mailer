name: KRX MA20 Close 15:40 (KR)

on:
  schedule:
    - cron: "40 6 * * 1-5"   # Mon-Fri 06:40 UTC = 15:40 KST
  workflow_dispatch:

concurrency:
  group: krx-ma20-close
  cancel-in-progress: true

jobs:
  scan-and-notify:
    runs-on: ubuntu-latest
    timeout-minutes: 15

    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install deps
        run: |
          python -m pip install --upgrade pip
          pip install -r requirements.txt

      - name: Run MA20 scan (KRX bulk)
        env:
          # 출력 설정
          TOP_N: "50"
          VALUE_FLOOR: "5000000000"   # 50억
          NEAR_PCT: "0.01"            # 1%

          # Telegram
          TELEGRAM_BOT_TOKEN: ${{ secrets.TELEGRAM_BOT_TOKEN }}
          TELEGRAM_CHAT_ID: ${{ secrets.TELEGRAM_CHAT_ID }}

          # Email (Gmail SMTP 권장)
          SMTP_HOST: "smtp.gmail.com"
          SMTP_PORT: "587"
          SMTP_USER: ${{ secrets.SMTP_USER }}
          SMTP_PASS: ${{ secrets.SMTP_PASS }}
          MAIL_TO:    ${{ secrets.MAIL_TO }}
        run: |
          python scripts/ma20_scan_kr.py
