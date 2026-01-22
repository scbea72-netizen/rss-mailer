from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List

import pandas as pd
from pykrx import stock


@dataclass(frozen=True)
class TradingDay:
    yyyymmdd: str


def _ymd(dt: datetime) -> str:
    return dt.strftime("%Y%m%d")


def recent_trading_days(n: int, end_date: str | None = None) -> List[TradingDay]:
    """
    ✅ 안정판:
    - pykrx의 get_market_ohlcv_by_ticker로 "거래일 여부"를 찍는 방식은 환경에 따라 0개가 나올 수 있어 불안정.
    - 그래서 '충분히 넓은 기간'을 잡고, 실제로 데이터가 있는 날짜만 뽑아 n개를 확보한다.
    """
    if n <= 0:
        return []

    if end_date:
        end_dt = datetime.strptime(end_date, "%Y%m%d")
    else:
        end_dt = datetime.now()

    # 넉넉히 2년치 캘린더 후보를 잡는다 (연휴/휴장/지연 대비)
    start_dt = end_dt - timedelta(days=730)

    start = _ymd(start_dt)
    end = _ymd(end_dt)

    # ✅ 핵심: "시장지수"로 날짜 존재 여부를 판별 (전종목 조회보다 훨씬 안정적)
    # KOSPI 지수(1001) 사용: 데이터 있으면 거래일
    try:
        idx = stock.get_index_ohlcv(start, end, "1001")
    except Exception as e:
        raise RuntimeError(f"지수 데이터 조회 실패: {e}")

    if idx is None or idx.empty:
        raise RuntimeError("지수 데이터가 비었습니다. (네트워크/pykrx 차단/일시 장애 가능)")

    # idx index가 날짜. 최신 n개 확보
    dates = [d.strftime("%Y%m%d") for d in idx.index]
    if len(dates) < n:
        raise RuntimeError(f"최근 거래일 {n}개를 확보하지 못했습니다. 확보={len(dates)}")

    dates = dates[-n:]  # 최신 n개
    return [TradingDay(d) for d in dates]


def fetch_bulk_ohlcv_for_date(date_yyyymmdd: str) -> pd.DataFrame:
    """
    특정 거래일의 KOSPI+KOSDAQ 전종목 OHLCV/거래대금(금액)을 '벌크'로 가져와 통합.
    반환 컬럼:
    - ticker, name, close, value, date, market
    """
    frames = []
    for mkt in ("KOSPI", "KOSDAQ"):
        df = stock.get_market_ohlcv_by_ticker(date_yyyymmdd, market=mkt)
        if df is None or df.empty:
            continue

        df = df.reset_index().rename(columns={"티커": "ticker"})
        if "거래대금" not in df.columns or "종가" not in df.columns:
            raise RuntimeError(f"pykrx 응답 컬럼이 예상과 다릅니다: {df.columns}")

        # 종목명 매핑은 비용이 크지 않지만, 간혹 느리면 캐시 확장 가능
        names = [stock.get_market_ticker_name(t) for t in df["ticker"].astype(str).tolist()]

        out = pd.DataFrame({
            "ticker": df["ticker"].astype(str),
            "name": pd.Series(names, dtype="string"),
            "close": pd.to_numeric(df["종가"], errors="coerce"),
            "value": pd.to_numeric(df["거래대금"], errors="coerce"),
            "date": date_yyyymmdd,
            "market": mkt,
        }).dropna(subset=["close", "value"])

        frames.append(out)

    if not frames:
        raise RuntimeError(f"{date_yyyymmdd} 데이터가 비었습니다(휴장일/지연 가능).")

    merged = pd.concat(frames, ignore_index=True)
    merged = merged.drop_duplicates(subset=["ticker", "date"], keep="last")
    return merged
