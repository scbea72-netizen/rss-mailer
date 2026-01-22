# scripts/utils_krx.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List

import pandas as pd
from pykrx import stock


@dataclass(frozen=True)
class TradingDay:
    yyyymmdd: str  # "YYYYMMDD"


def _is_weekend(dt: datetime) -> bool:
    return dt.weekday() >= 5


def recent_trading_days(n: int, end_date: str | None = None, lookback_calendar_days: int = 60) -> List[TradingDay]:
    """
    최근 n개 '거래일'을 반환.
    - end_date 없으면 오늘(서울 기준이든 상관없이) 날짜로부터 역산
    - 주말/휴일은 pykrx에서 데이터가 비면 자동 skip
    """
    if n <= 0:
        return []

    if end_date:
        end_dt = datetime.strptime(end_date, "%Y%m%d")
    else:
        end_dt = datetime.now()

    days: List[TradingDay] = []
    seen = set()

    dt = end_dt
    min_dt = end_dt - timedelta(days=lookback_calendar_days)

    while dt >= min_dt and len(days) < n:
        if _is_weekend(dt):
            dt -= timedelta(days=1)
            continue

        d = dt.strftime("%Y%m%d")
        if d in seen:
            dt -= timedelta(days=1)
            continue

        # KOSPI로 한 번 찍어보고 데이터가 있으면 거래일로 인정
        try:
            df = stock.get_market_ohlcv_by_ticker(d, market="KOSPI")
            if df is not None and len(df) > 0:
                days.append(TradingDay(d))
                seen.add(d)
        except Exception:
            # 네트워크/일시 오류는 다음날로 넘어가며 계속 시도
            pass

        dt -= timedelta(days=1)

    # 오래된 날짜 → 최신 날짜 순으로 정렬
    days_sorted = sorted(days, key=lambda x: x.yyyymmdd)
    if len(days_sorted) < n:
        raise RuntimeError(f"최근 거래일 {n}개를 확보하지 못했습니다. 확보={len(days_sorted)} lookback={lookback_calendar_days}일")
    return days_sorted


def fetch_bulk_ohlcv_for_date(date_yyyymmdd: str) -> pd.DataFrame:
    """
    특정 거래일의 KOSPI+KOSDAQ 전종목 OHLCV/거래대금(금액)을 '벌크'로 가져와 통합.
    반환 컬럼(표준화):
    - ticker, name, close, value, date
    """
    frames = []
    for mkt in ("KOSPI", "KOSDAQ"):
        df = stock.get_market_ohlcv_by_ticker(date_yyyymmdd, market=mkt)
        if df is None or df.empty:
            continue

        # pykrx 컬럼: 시가/고가/저가/종가/거래량/거래대금/시가총액 등
        # 여기선 종가/거래대금 중심으로 사용
        df = df.reset_index().rename(columns={"티커": "ticker"})
        if "거래대금" not in df.columns or "종가" not in df.columns:
            raise RuntimeError(f"pykrx 응답 컬럼이 예상과 다릅니다: {df.columns}")

        df["name"] = df["ticker"].apply(stock.get_market_ticker_name)

        out = pd.DataFrame({
            "ticker": df["ticker"].astype(str),
            "name": df["name"].astype(str),
            "close": pd.to_numeric(df["종가"], errors="coerce"),
            "value": pd.to_numeric(df["거래대금"], errors="coerce"),
            "date": date_yyyymmdd,
            "market": mkt,
        })
        out = out.dropna(subset=["close", "value"])
        frames.append(out)

    if not frames:
        raise RuntimeError(f"{date_yyyymmdd} 데이터가 비었습니다(휴장일 가능).")

    merged = pd.concat(frames, ignore_index=True)
    # 혹시 중복이 생기면 ticker+date 기준으로 마지막 유지
    merged = merged.drop_duplicates(subset=["ticker", "date"], keep="last")
    return merged
