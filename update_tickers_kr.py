from datetime import datetime
from pykrx import stock

def main():
    today = datetime.now().strftime("%Y%m%d")
    kospi = stock.get_market_ticker_list(today, market="KOSPI")
    kosdaq = stock.get_market_ticker_list(today, market="KOSDAQ")

    lines = [f"{c}.KS" for c in kospi] + [f"{c}.KQ" for c in kosdaq]
    lines = sorted(set(lines))

    with open("tickers_kr.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print("KR tickers generated:", len(lines))

if __name__ == "__main__":
    main()
