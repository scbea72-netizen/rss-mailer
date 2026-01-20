#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
scan_close_kr.py
- KOSPI + KOSDAQ 전체 종목을 일봉(장마감 기준)으로 스캔
- 기본 조건:
  1) 종가 > 20일 이동평균(MA20)
  2) RSI(14) >= 55
  3) MACD Histogram >= 0
  4) 일간 등락률(%) >= 3.0 (기본값, 인자에서 조정 가능)
- 옵션:
  --use-volume   거래량 조건을 추가(기본 OFF)
  --vol-mult     거래량 조건 배수 (기본 1.3배, 20일 평균 대비)
  --min-price    최소 주가 필터 (기본 1000원)
  --top          결과 상위 N개만 출력(기본 50)

의존성:
  pip install pandas numpy yfinance FinanceDataReader

권장 실행(장 마감 후):
  python scan_close_kr.py --min-change 3 --top 80
"""

import argparse
import sys
import math
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

# yfinance는 일봉을 안정적으로 가져오기 좋음
import yfinance as yf

# FinanceDataReader는 KRX 전체 티커 확보에 사용
try:
    import FinanceDataReader as fdr
except Exception:
    fdr = None


def ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()


def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)

    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()

    rs = avg_gain / (avg_loss.replace(0, np.nan))
    rsi_val = 100 - (100 / (1 + rs))
    return rsi_val.fillna(0)


def macd_hist(close: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> pd.Series:
    macd_line = ema(close, fast) - ema(close, slow)
    signal_line = ema(macd_line, signal)
    hist = macd_line - signal_line
    return hist


def get_krx_tickers() -> pd.DataFrame:
    """
    Returns DataFrame with columns: ['Code', 'Name', 'Market']
    Market: 'KOSPI' or 'KOSDAQ'
    """
    if fdr is None:
        raise RuntimeError("FinanceDataReader가 설치되어 있지 않습니다. `pip install FinanceDataReader` 해주세요.")

    kospi = fdr.StockListing("KRX")  # KRX 전체(코스피+코스닥+코넥스 등) 포함될 수 있음
    # kospi columns often: Code, Name, Market, ...
    needed = {"Code", "Name"}
    if not needed.issubset(set(kospi.columns)):
        raise RuntimeError(f"StockListing('KRX') 컬럼이 예상과 다릅니다: {kospi.columns}")

    # Market 컬럼이 있으면 코스피/코스닥만 필터
    if "Market" in kospi.columns:
        df = kospi.copy()
        df = df[df["Market"].isin(["KOSPI", "KOSDAQ"])].copy()
        df = df[["Code", "Name", "Market"]].dropna()
        df["Code"] = df["Code"].astype(str).str.zfill(6)
        return df.reset_index(drop=True)

    # Market 컬럼이 없으면: KOSPI/KOSDAQ 따로 받아 합치기
    k1 = fdr.StockListing("KOSPI")[["Code", "Name"]].copy()
    k1["Market"] = "KOSPI"
    k2 = fdr.StockListing("KOSDAQ")[["Code", "Name"]].copy()
    k2["Market"] = "KOSDAQ"
    df = pd.concat([k1, k2], ignore_index=True)
    df["Code"] = df["Code"].astype(str).str.zfill(6)
    return df.reset_index(drop=True)


def yf_symbol_from_code(code: str, market: str) -> str:
    # yfinance 한국 심볼 규칙: 005930.KS (KOSPI), 035720.KQ (KOSDAQ)
    suffix = ".KS" if market == "KOSPI" else ".KQ"
    return f"{str(code).zfill(6)}{suffix}"


def fetch_ohlcv_yf(symbol: str, lookback_days: int = 220) -> pd.DataFrame:
    """
    yfinance로 일봉 OHLCV 불러오기
    """
    end = datetime.utcnow().date() + timedelta(days=1)
    start = end - timedelta(days=lookback_days)

    df = yf.download(
        symbol,
        start=start.isoformat(),
        end=end.isoformat(),
        interval="1d",
        auto_adjust=False,
        progress=False,
        threads=False,
    )

    if df is None or df.empty:
        return pd.DataFrame()

    # yfinance 컬럼 표준화
    df = df.rename(columns={
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Adj Close": "adj_close",
        "Volume": "volume",
    })

    # 인덱스 날짜
    df.index = pd.to_datetime(df.index)
    df = df.sort_index()
    return df


def compute_signals(df: pd.DataFrame) -> dict:
    """
    df: columns must include close, volume
    return dict of latest indicators
    """
    if df.shape[0] < 60:
        return {}

    close = df["close"].astype(float)
    volume = df["volume"].astype(float)

    ma20 = close.rolling(20).mean()
    ma60 = close.rolling(60).mean()

    rsi14 = rsi(close, 14)
    hist = macd_hist(close, 12, 26, 9)

    # 일간 등락률
    change_pct = close.pct_change() * 100.0

    # 거래량 평균(20일)
    vol_ma20 = volume.rolling(20).mean()

    last = df.index[-1]
    out = {
        "date": last.strftime("%Y-%m-%d"),
        "close": float(close.iloc[-1]),
        "change_pct": float(change_pct.iloc[-1]) if not np.isnan(change_pct.iloc[-1]) else 0.0,
        "ma20": float(ma20.iloc[-1]) if not np.isnan(ma20.iloc[-1]) else 0.0,
        "ma60": float(ma60.iloc[-1]) if not np.isnan(ma60.iloc[-1]) else 0.0,
        "rsi14": float(rsi14.iloc[-1]),
        "macd_hist": float(hist.iloc[-1]) if not np.isnan(hist.iloc[-1]) else 0.0,
        "volume": float(volume.iloc[-1]) if not np.isnan(volume.iloc[-1]) else 0.0,
        "vol_ma20": float(vol_ma20.iloc[-1]) if not np.isnan(vol_ma20.iloc[-1]) else 0.0,
    }
    return out


def passes_filters(sig: dict,
                   min_change: float,
                   min_price: float,
                   require_ma20_break: bool,
                   require_macd_pos: bool,
                   rsi_min: float,
                   use_volume: bool,
                   vol_mult: float) -> bool:
    if not sig:
        return False

    close = sig["close"]
    if close < min_price:
        return False

    if sig["change_pct"] < min_change:
        return False

    if require_ma20_break and not (sig["close"] > sig["ma20"]):
        return False

    if require_macd_pos and not (sig["macd_hist"] >= 0):
        return False

    if sig["rsi14"] < rsi_min:
        return False

    if use_volume:
        # 20일 평균 대비 vol_mult 배 이상
        if sig["vol_ma20"] <= 0:
            return False
        if sig["volume"] < sig["vol_ma20"] * vol_mult:
            return False

    return True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--min-change", type=float, default=3.0, help="일간 등락률 최소(%) (기본 3.0)")
    ap.add_argument("--min-price", type=float, default=1000.0, help="최소 종가(원) (기본 1000)")
    ap.add_argument("--rsi-min", type=float, default=55.0, help="RSI(14) 최소 (기본 55)")
    ap.add_argument("--top", type=int, default=50, help="상위 N개 출력 (기본 50)")
    ap.add_argument("--use-volume", action="store_true", help="거래량 조건 추가(기본 OFF)")
    ap.add_argument("--vol-mult", type=float, default=1.3, help="거래량 배수(20일평균 대비) (기본 1.3)")
    ap.add_argument("--no-ma20", action="store_true", help="MA20 돌파 조건 끄기")
    ap.add_argument("--no-macd", action="store_true", help="MACD 히스토그램 양수 조건 끄기")
    ap.add_argument("--limit", type=int, default=0, help="테스트용: 티커 N개만(0이면 전체)")
    args = ap.parse_args()

    require_ma20 = not args.no_ma20
    require_macd = not args.no_macd

    try:
        tickers_df = get_krx_tickers()
    except Exception as e:
        print(f"[ERROR] 티커 리스트 확보 실패: {e}", file=sys.stderr)
        sys.exit(1)

    if args.limit and args.limit > 0:
        tickers_df = tickers_df.head(args.limit).copy()

    results = []
    total = len(tickers_df)

    for i, row in tickers_df.iterrows():
        code = str(row["Code"]).zfill(6)
        name = str(row["Name"])
        market = str(row["Market"])

        symbol = yf_symbol_from_code(code, market)
        df = fetch_ohlcv_yf(symbol, lookback_days=260)

        if df.empty:
            continue

        sig = compute_signals(df)
        if not sig:
            continue

        if passes_filters(
            sig=sig,
            min_change=args.min_change,
            min_price=args.min_price,
            require_ma20_break=require_ma20,
            require_macd_pos=require_macd,
            rsi_min=args.rsi_min,
            use_volume=args.use_volume,
            vol_mult=args.vol_mult
        ):
            results.append({
                "code": code,
                "name": name,
                "market": market,
                "symbol": symbol,
                "date": sig["date"],
                "close": sig["close"],
                "chg%": sig["change_pct"],
                "ma20": sig["ma20"],
                "rsi14": sig["rsi14"],
                "macd_hist": sig["macd_hist"],
                "vol": sig["volume"],
                "vol_ma20": sig["vol_ma20"],
            })

        # 너무 조용하면 진행 표시(대량 스캔이라 체감상 필요할 때가 많음)
        if (i + 1) % 400 == 0:
            print(f"...progress {i+1}/{total}", file=sys.stderr)

    if not results:
        print("NO SIGNALS")
        return

    out = pd.DataFrame(results)

    # 우선순위: 등락률 높은 순 -> RSI 높은 순
    out = out.sort_values(["chg%", "rsi14"], ascending=[False, False]).reset_index(drop=True)

    # 보기 좋게 라운딩
    out["close"] = out["close"].round(0).astype(int)
    out["chg%"] = out["chg%"].round(2)
    out["ma20"] = out["ma20"].round(0).astype(int)
    out["rsi14"] = out["rsi14"].round(1)
    out["macd_hist"] = out["macd_hist"].round(4)

    # 상위 N
    top_n = min(args.top, len(out))
    out2 = out.head(top_n).copy()

    # 출력
    header = (
        f"[KR CLOSE SCAN] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | "
        f"min_change={args.min_change}% | rsi>={args.rsi_min} | "
        f"MA20={'ON' if require_ma20 else 'OFF'} | MACD={'ON' if require_macd else 'OFF'} | "
        f"VOL={'ON' if args.use_volume else 'OFF'}"
    )
    print(header)
    print("-" * len(header))

    # 표 출력
    cols = ["market", "code", "name", "close", "chg%", "ma20", "rsi14", "macd_hist"]
    if args.use_volume:
        cols += ["vol", "vol_ma20"]

    # 고정폭으로 보기 좋게
    print(out2[cols].to_string(index=False))

    # 참고용: 심볼/날짜도 필요하면 아래 주석 해제
    # print("\nSymbols:")
    # for _, r in out2.iterrows():
    #     print(f"{r['code']} {r['name']} ({r['symbol']}) {r['date']} chg={r['chg%']}%")


if __name__ == "__main__":
    main()
