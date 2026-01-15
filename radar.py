import os
import pandas as pd
import numpy as np
import yfinance as yf
import requests
from datetime import datetime, timezone, timedelta

KST = timezone(timedelta(hours=9))

TICKERS = [
    "005930.KS", "000660.KS", "042700.KQ", "039030.KQ", "036930.KQ"
]

TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")

def send_telegram(msg):
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    requests.post(url, data={"chat_id": TG_CHAT_ID, "text": msg})

def main():
    now = datetime.now(KST).strftime("%Y-%m-%d %H:%M")
    hits = []

    for t in TICKERS:
        df = yf.download(t, period="3mo", interval="1d", progress=False)
        if len(df) < 30: continue

        df["MA20"] = df["Close"].rolling(20).mean()
        df["VOL20"] = df["Volume"].rolling(20).mean()

        last = df.iloc[-1]
        prev = df.iloc[-2]

        if prev["Close"] <= prev["MA20"] and last["Close"] > last["MA20"] and last["Volume"] >= 3 * last["VOL20"]:
            change = (last["Close"] / prev["Close"] - 1) * 100
            if change >= 8:
                hits.append(f"{t}  {change:.1f}%  종가 {int(last['Close'])}")

    if hits:
        send_telegram("🚨 20일선 돌파 + 거래량 폭증\n" + "\n".join(hits))
    else:
        print("No hits", now)

if __name__ == "__main__":
    main()
