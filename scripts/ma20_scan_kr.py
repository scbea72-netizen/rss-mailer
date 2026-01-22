from pathlib import Path
import pandas as pd
import os

from utils_krx import recent_trading_days, fetch_bulk_ohlcv_for_date
from notify import send_email, send_telegram

CACHE = Path(".cache")
CACHE.mkdir(exist_ok=True)


def main():
    TOP_N = int(os.environ.get("TOP_N", 50))
    VALUE_FLOOR = int(os.environ.get("VALUE_FLOOR", 5_000_000_000))
    NEAR_PCT = float(os.environ.get("NEAR_PCT", 0.01))
    DAYS = int(os.environ.get("TRADING_DAYS", 25))

    days = recent_trading_days(DAYS)
    day_list = [d.yyyymmdd for d in days]

    frames = []
    for d in day_list:
        fp = CACHE / f"kis_{d}.csv"
        if fp.exists():
            df = pd.read_csv(fp)
        else:
            df = fetch_bulk_ohlcv_for_date(d)
            df.to_csv(fp, index=False)
        frames.append(df)

    hist = pd.concat(frames)
    pivot_close = hist.pivot(index="date", columns="ticker", values="close")
    pivot_value = hist.pivot(index="date", columns="ticker", values="value")

    ma20 = pivot_close.rolling(20).mean()

    prev, now = pivot_close.index[-2], pivot_close.index[-1]

    breakout = (
        (pivot_close.loc[prev] <= ma20.loc[prev])
        & (pivot_close.loc[now] > ma20.loc[now])
        & (pivot_value.loc[now] >= VALUE_FLOOR)
    )

    near = (
        (abs(pivot_close.loc[now] - ma20.loc[now]) / ma20.loc[now] <= NEAR_PCT)
        & (pivot_value.loc[now] >= VALUE_FLOOR)
    )

    names = (
        hist[hist["date"] == now][["ticker", "name"]]
        .drop_duplicates()
        .set_index("ticker")["name"]
        .to_dict()
    )

    def make_df(mask):
        tickers = mask[mask].index.tolist()
        if not tickers:
            return pd.DataFrame()
        return (
            pd.DataFrame(
                {
                    "ticker": tickers,
                    "name": [names.get(t, "") for t in tickers],
                    "close": pivot_close.loc[now, tickers],
                    "ma20": ma20.loc[now, tickers],
                    "value": pivot_value.loc[now, tickers],
                }
            )
            .sort_values("value", ascending=False)
            .head(TOP_N)
        )

    df_break = make_df(breakout)
    df_near = make_df(near)

    body = f"[KR MA20] {now}\n\n돌파\n{df_break}\n\n근접\n{df_near}"
    send_email(f"[KR] MA20 종가 {now}", body)
    send_telegram(body)

    print(body)


if __name__ == "__main__":
    main()
