from datetime import datetime, timedelta
from pykrx import stock

def find_latest_trading_day(max_back=14):
    d = datetime.now()
    for _ in range(max_back):
        day = d.strftime("%Y%m%d")
        try:
            kospi = stock.get_market_ticker_list(day, market="KOSPI")
            kosdaq = stock.get_market_ticker_list(day, market="KOSDAQ")
            if kospi and kosdaq:
                return day
        except Exception:
            pass
        d -= timedelta(days=1)
    raise RuntimeError("최근 14일 이내에 거래일을 찾지 못했습니다.")

def main():
    day = find_latest_trading_day()

    kospi = stock.get_market_ticker_list(day, market="KOSPI")
    kosdaq = stock.get_market_ticker_list(day, market="KOSDAQ")

    lines = [f"{c}.KS" for c in kospi] + [f"{c}.KQ" for c in kosdaq]
    lines = sorted(set(lines))

    with open("tickers_kr.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

    print(f"[OK] trading_day={day} tickers={len(lines)}")

if __name__ == "__main__":
    main()
