import yfinance as yf
import json

FILES = {
    "KR": "tickers_kr.txt",
    "JP": "tickers_jp.txt",
    "US": "tickers_us.txt"
}

OUT = "ticker_names.json"

names = {}

for mkt, fn in FILES.items():
    try:
        with open(fn, encoding="utf-8") as f:
            tickers = [l.strip() for l in f if l.strip()]
    except:
        continue

    for t in tickers:
        try:
            info = yf.Ticker(t).info
            nm = info.get("shortName") or info.get("longName")
            if nm:
                names[t] = nm
        except:
            pass

with open(OUT, "w", encoding="utf-8") as f:
    json.dump(names, f, ensure_ascii=False, indent=2)

print("기업명 DB 생성:", len(names))
