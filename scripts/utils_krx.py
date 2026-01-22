from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional
import time
import random
from datetime import datetime, timedelta

import pandas as pd
import exchange_calendars as ecals
from pykrx import stock


@dataclass(frozen=True)
class TradingDay:
    yyyymmdd: str


def _kst_business_asof_date(now_kst: Optional[datetime] = None) -> str:
    """
    KST 기준 '기준일(asof)'을 안전하게 계산.
    - 장중(16시 이전)에는 전일 종가 기준으로 계산하는 게 안전
    - 장마감 이후에는 당일 기준
    """
    if now_kst is None:
        now_kst = datetime.now()

    # now_kst가 naive일 수도 있으니 로컬 KST로 간주 (Actions에서 TZ=Asia/Seoul 권장)
    # 16:00 이전이면 전일로
    if now_kst.hour < 16:
        d = (now_kst.date() - timedelta(days=1))
    else:
        d = now_kst.date()

    return d.strftime("%Y%m%d")


def recent_trading_days(n: int, end_date: str | None = None, back_days: int = 365) -> List[TradingDay]:
    """
    exchange_calendars(XKRX)로 최근 거래일 n개를 뽑는다.
    - end_date가 없으면 KST 기준으로 '장중이면 전일, 장마감 후면 당일'을 end로 사용
    - sessions_in_range는 UTC tz를 쓰므로, end는 해당 날짜를 포함하도록 range를 넉넉히 잡는다.
    """
    if n <= 0:
        return []

    cal = ecals.get_calendar("XKRX")

    if end_date:
        # YYYYMMDD 또는 YYYY-MM-DD 모두 허용
        if "-" in end_date:
            end_kst = pd.Timestamp(end_date).date()
        else:
            end_kst = pd.Timestamp(end_date).date()
        end_yyyymmdd = pd.Timestamp(end_kst).strftime("%Y%m%d")
    else:
        end_yyyymmdd = _kst_business_asof_date()

    # UTC 범위로 넉넉히 잡아서 해당 KST 거래일 세션이 포함되게 함
    # (KST 날짜의 23:59를 UTC로 변환해 end로 사용)
    end_ts_kst = pd.Timestamp(end_yyyymmdd).tz_localize("Asia/Seoul") + pd.Timedelta(hours=23, minutes=59)
    end_utc = end_ts_kst.tz_convert("UTC")

    start_utc = end_utc - pd.Timedelta(days=back_days)

    sessions = cal.sessions_in_range(start_utc, end_utc)
    if sessions is None or len(sessions) < n:
        raise RuntimeError(f"최근 거래일 {n}개를 확보하지 못했습니다. 확보={0 if sessions is None else len(sessions)}")

    days = sessions[-n:]
    return [TradingDay(d.strftime("%Y%m%d")) for d in days]


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

    ⚠️ 주의:
    - KRX가 일시 차단/장애면 특정 날짜에서 실패할 수 있음
    - 상위 로직에서 "실패 날짜는 스킵하고 전체를 살리는" 방식으로 운영 권장
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

            sleep_s = min(60, (2 ** (attempt - 1))) + random.random() * 2.0  # 지터 강화
            print(f"[WARN] pykrx fetch failed/invalid ({market}) date={date_yyyymmdd} attempt={attempt}/{max_retry} sleep={sleep_s:.1f}s")
            time.sleep(sleep_s)

        if df is None:
            print(f"[ERROR] pykrx fetch FAILED ({market}) date={date_yyyymmdd} after {max_retry} retries")
            continue

        df = df.reset_index().rename(columns={"티커": "ticker"})
        df["ticker"] = df["ticker"].astype(str)

        tickers = df["ticker"].tolist()

        # 종목명은 느릴 수 있으니 실패해도 진행
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

        # 시장 2개 연속 호출이어서 KRX 차단 줄이기 위해 아주 짧은 쿨다운
        time.sleep(0.2 + random.random() * 0.3)

    if not frames:
        raise RuntimeError(f"{date_yyyymmdd} 데이터 수집 전체 실패(차단/장애/휴장 가능)")

    merged = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["ticker", "date"], keep="last")
    return merged
