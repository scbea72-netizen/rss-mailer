from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional
import time
import random
from datetime import datetime, timedelta

import pandas as pd
from pykrx import stock


@dataclass(frozen=True)
class TradingDay:
    yyyymmdd: str


def _kst_business_asof_date(now: Optional[datetime] = None) -> str:
    """
    KST 기준 안전한 기준일(asof) 계산
    - 16:00 이전(장중)에는 전일 기준
    - 16:00 이후에는 당일 기준
    """
    if now is None:
        now = datetime.now()

    if now.hour < 16:
        d = now.date() - timedelta(days=1)
    else:
        d = now.date()

    return d.strftime("%Y%m%d")


def _fallback_weekdays(n: int, asof: str, back_days: int = 120) -> List[str]:
    """
    ✅ 최후 fallback: 최근 '평일' 기준으로 날짜 후보를 만든다.
    - 휴장일이 섞여도 이후 OHLCV 수집에서 스킵되도록 설계되어 있음
    """
    end = pd.Timestamp(asof).date()
    start = (pd.Timestamp(asof) - pd.Timedelta(days=back_days)).date()

    days = []
    d = start
    while d <= end:
        if d.weekday() < 5:  # Mon~Fri
            days.append(pd.Timestamp(d).strftime("%Y%m%d"))
        d += timedelta(days=1)

    if len(days) < n:
        # back_days를 늘려도 되지만, 여기선 최대한 확보만 해두기
        return days
    return days[-n:]


def recent_trading_days(n: int, end_date: str | None = None, back_days: int = 365) -> List[TradingDay]:
    """
    ✅ 거래일 확보 로직(안깨짐)
    1) 1순위: pykrx.get_previous_business_days (정상일 때 가장 정확)
    2) 2순위: 실패/빈값이면 최근 '평일' 리스트로 fallback (휴장일은 이후 수집에서 스킵)
    """
    if n <= 0:
        return []

    # end_date 옵션 처리
    if end_date:
        if "-" in end_date:
            asof = pd.Timestamp(end_date).strftime("%Y%m%d")
        else:
            asof = str(end_date)
    else:
        asof = _kst_business_asof_date()

    start = (pd.Timestamp(asof) - pd.Timedelta(days=back_days)).strftime("%Y%m%d")

    days = []
    try:
        days = stock.get_previous_business_days(fromdate=start, todate=asof) or []
    except Exception as e:
        print(f"[WARN] get_previous_business_days failed: {e}")
        days = []

    # ✅ pykrx가 빈 리스트를 주는 경우가 있어서 fallback
    if not days or len(days) < n:
        print(f"[WARN] trading_days from pykrx is empty/insufficient. fallback to weekdays. got={len(days)} need={n}")
        fb = _fallback_weekdays(n=n, asof=asof, back_days=max(120, n * 4))
        if not fb or len(fb) < n:
            # 그래도 부족하면 있는 만큼이라도 반환 (상위에서 스킵/부족 체크)
            print(f"[WARN] fallback weekdays insufficient: got={0 if not fb else len(fb)} need={n}")
            days = fb
        else:
            days = fb

    return [TradingDay(d) for d in days[-n:]]


def _get_market_ohlcv_safe(date_yyyymmdd: str, market: str) -> pd.DataFrame | None:
    """
    pykrx 호출을 안전하게 감싸는 래퍼:
    - 일시 장애/차단/빈 응답 시 None 반환
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

        for attempt in range(1, max_retry + 1):
            df = _get_market_ohlcv_safe(date_yyyymmdd, market)

            if df is not None and len(df.columns) > 0:
                cols = set(map(str, df.columns))
                if required_cols.issubset(cols):
                    break
                df = None

            sleep_s = min(60, (2 ** (attempt - 1))) + random.random() * 2.0
            print(f"[WARN] pykrx fetch failed/invalid ({market}) date={date_yyyymmdd} attempt={attempt}/{max_retry} sleep={sleep_s:.1f}s")
            time.sleep(sleep_s)

        if df is None:
            print(f"[ERROR] pykrx fetch FAILED ({market}) date={date_yyyymmdd} after {max_retry} retries")
            continue

        df = df.reset_index().rename(columns={"티커": "ticker"})
        df["ticker"] = df["ticker"].astype(str)

        tickers = df["ticker"].tolist()

        names = []
        for t in tickers:
            try:
                names.append(stock.get_market_ticker_name(t))
            except Exception:
                names.append("")

        out = pd.DataFrame(
            {
                "ticker": tickers,
                "name": names,
                "close": pd.to_numeric(df.get("종가"), errors="coerce"),
                "value": pd.to_numeric(df.get("거래대금"), errors="coerce"),
                "date": date_yyyymmdd,
                "market": market,
            }
        ).dropna(subset=["close", "value"])

        frames.append(out)

        time.sleep(0.2 + random.random() * 0.3)

    if not frames:
        raise RuntimeError(f"{date_yyyymmdd} 데이터 수집 전체 실패(차단/장애/휴장 가능)")

    merged = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["ticker", "date"], keep="last")
    return merged
