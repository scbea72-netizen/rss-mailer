from __future__ import annotations

from dataclasses import dataclass
from typing import List
import time
import random

import pandas as pd
import exchange_calendars as ecals
from pykrx import stock


@dataclass(frozen=True)
class TradingDay:
    yyyymmdd: str


def recent_trading_days(n: int, end_date: str | None = None) -> List[TradingDay]:
    if n <= 0:
        return []

    cal = ecals.get_calendar("XKRX")

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


def _get_market_ohlcv_safe(date_yyyymmdd: str, market: str) -> pd.DataFrame | None:
    """
    pykrx 호출을 안전하게 감싸는 래퍼:
    - 일시 장애/차단/빈 응답 시 None 반환
    - retry/backoff는 상위에서 처리
    """
    try:
        df = stock.get_market_ohlcv_by_ticker(date_yyyymmdd, market=market)
        if df is None or df.empty:
            return None
        return df
    except Exception:
        return None


def fetch_bulk_ohlcv_for_date(date_yyyymmdd: str, max_retry: int = 6) -> pd.DataFrame:
    """
    ✅ 운영급(안깨짐) 벌크 수집
    - pykrx/KRX 응답 흔들림 대비: 재시도 + 컬럼검증 + 빈응답 방어
    - 반환 컬럼: ticker, name, close, value, date, market
    """
    frames = []
    required_cols = {"시가", "고가", "저가", "종가", "거래대금"}

    for market in ("KOSPI", "KOSDAQ"):
        df = None

        # 재시도 + 지수 백오프(1s,2s,4s...) + 지터
        for attempt in range(1, max_retry + 1):
            df = _get_market_ohlcv_safe(date_yyyymmdd, market)

            if df is not None and len(df.columns) > 0:
                # pykrx가 컬럼을 다르게 주거나 깨진 경우 방어
                cols = set(map(str, df.columns))
                if required_cols.issubset(cols):
                    break  # 성공
                else:
                    df = None  # 깨진 응답 취급

            sleep_s = min(60, (2 ** (attempt - 1))) + random.random()
            print(f"[WARN] pykrx fetch failed/invalid ({market}) date={date_yyyymmdd} attempt={attempt}/{max_retry} sleep={sleep_s:.1f}s")
            time.sleep(sleep_s)

        if df is None:
            # 이 시장은 포기(하지만 다른 시장이라도 모으면 진행)
            print(f"[ERROR] pykrx fetch FAILED ({market}) date={date_yyyymmdd} after {max_retry} retries")
            continue

        df = df.reset_index().rename(columns={"티커": "ticker"})
        df["ticker"] = df["ticker"].astype(str)

        tickers = df["ticker"].tolist()

        # 종목명은 종종 느리므로: 실패해도 빈값으로
        names = []
        for t in tickers:
            try:
                names.append(stock.get_market_ticker_name(t))
            except Exception:
                names.append("")

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
        raise RuntimeError(f"{date_yyyymmdd} 데이터 수집 전체 실패(차단/장애/휴장 가능)")

    merged = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["ticker", "date"], keep="last")
    return merged
