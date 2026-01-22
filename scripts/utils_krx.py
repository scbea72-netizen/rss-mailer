from __future__ import annotations

from dataclasses import dataclass
from typing import List

import pandas as pd
import exchange_calendars as ecals
from pykrx import stock


@dataclass(frozen=True)
class TradingDay:
    yyyymmdd: str


def recent_trading_days(n: int, end_date: str | None = None) -> List[TradingDay]:
    """
    ✅ 최종 안정판 (영원히 안 깨짐)
    - 거래일 계산: exchange_calendars(XKRX)로 로컬 계산 (네트워크/pykrx 지수 API 의존 없음)
    - exchange_calendars는 tz-aware Timestamp에 민감하므로 tz-naive 날짜만 전달
    """
    if n <= 0:
        return []

    cal = ecals.get_calendar("XKRX")

    # exchange_calendars에는 timezone 없는 (tz-naive) 날짜만 넘긴다
    if end_date:
        end = pd.Timestamp(end_date).normalize()
    else:
        end = pd.Timestamp.now().normalize()

    start = end - pd.Timedelta(days=365)

    sessions = cal.sessions_in_range(start, end)

    if sessions is None or len(sessions) < n:
        raise RuntimeError(f"최근 거래일 {n}개를 확보하지 못했습니다. 확보={0 if sessions is None else len(sessions)}")

    days = sessions[-n:]
    return [TradingDay(d.strftime("%Y%m%d")) for d in days]


def fetch_bulk_ohlcv_for_date(date_yyyymmdd: str) -> pd.DataFrame:
    """
    특정 거래일의 KOSPI+KOSDAQ 전종목 OHLCV/거래대금(금액)을 벌크로 가져와 통합.

    반환 컬럼:
    - ticker, name, close, value, date, market
    """
    frames = []

    for market in ("KOSPI", "KOSDAQ"):
        df = stock.get_market_ohlcv_by_ticker(date_yyyymmdd, market=market)

        if df is None or df.empty:
            continue

        df = df.reset_index().rename(columns={"티커": "ticker"})

        # pykrx 컬럼명 방어
        if "종가" not in df.columns or "거래대금" not in df.columns:
            raise RuntimeError(f"pykrx 응답 컬럼이 예상과 다릅니다: {list(df.columns)}")

        tickers = df["ticker"].astype(str).tolist()

        # 종목명 매핑 (필요시 향후 캐시 가능)
        names = [stock.get_market_ticker_name(t) for t in tickers]

        out = pd.DataFrame({
            "ticker": tickers,
            "name": names,
            "close": pd.to_numeric(df["종가"], errors="coerce"),
            "value": pd.to_numeric(df["거래대금"], errors="coerce"),
            "date": date_yyyymmdd,
            "market": market,
        }).dropna(subset=["close", "value"])

        frames.append(out)

    if not frames:
        raise RuntimeError(f"{date_yyyymmdd} 데이터 수집 실패(휴장/지연/차단 가능)")

    merged = pd.concat(frames, ignore_index=True)
    merged = merged.drop_duplicates(subset=["ticker", "date"], keep="last")
    return merged
