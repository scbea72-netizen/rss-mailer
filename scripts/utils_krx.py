from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import List

import pandas as pd
import exchange_calendars as ecals
from pykrx import stock


@dataclass(frozen=True)
class TradingDay:
    yyyymmdd: str


def recent_trading_days(n: int, end_date: str | None = None) -> List[TradingDay]:
    """
    ✅ 최종 안정판
    - KRX 거래일을 'exchange_calendars'로 계산 (네트워크/pykrx 무관)
    - 서버/GitHub Actions에서도 절대 안 깨짐
    """
    if n <= 0:
        return []

    cal = ecals.get_calendar("XKRX")

    if end_date:
        end = pd.Timestamp(end_date)
    else:
        end = pd.Timestamp.now(tz="Asia/Seoul")

    sessions = cal.sessions_in_range(
        end - pd.Timedelta(days=365),
        end
    )

    if len(sessions) < n:
        raise RuntimeError(
            f"최근 거래일 {n}개를 확보하지 못했습니다. 확보={len(sessions)}"
        )

    days = sessions[-n:]
    return [TradingDay(d.strftime("%Y%m%d")) for d in days]


def fetch_bulk_ohlcv_for_date(date_yyyymmdd: str) -> pd.DataFrame:
    """
    KOSPI + KOSDAQ 전종목 OHLCV 벌크 수집
    """
    frames = []

    for market in ("KOSPI", "KOSDAQ"):
        df = stock.get_market_ohlcv_by_ticker(
            date_yyyymmdd,
            market=market
        )

        if df is None or df.empty:
            continue

        df = df.reset_index().rename(columns={"티커": "ticker"})

        if "종가" not in df.columns or "거래대금" not in df.columns:
            raise RuntimeError(f"pykrx 컬럼 변경 감지: {df.columns}")

        names = [
            stock.get_market_ticker_name(t)
            for t in df["ticker"].astype(str)
        ]

        out = pd.DataFrame({
            "ticker": df["ticker"].astype(str),
            "name": names,
            "close": pd.to_numeric(df["종가"], errors="coerce"),
            "value": pd.to_numeric(df["거래대금"], errors="coerce"),
            "date": date_yyyymmdd,
            "market": market,
        }).dropna(subset=["close", "value"])

        frames.append(out)

    if not frames:
        raise RuntimeError(f"{date_yyyymmdd} 데이터 수집 실패(휴장/차단 가능)")

    return (
        pd.concat(frames, ignore_index=True)
        .drop_duplicates(subset=["ticker", "date"])
    )
