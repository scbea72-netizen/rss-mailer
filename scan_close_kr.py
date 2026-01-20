#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
scan_close_kr.py (KR close scan)
- KOSPI + KOSDAQ 전체 종목을 일봉(장마감 기준)으로 스캔
- 기본 필터:
  1) 종가 > MA20
  2) RSI(14) >= 55
  3) MACD Histogram >= 0
  4) 일간 등락률(%) >= 2.0 (워크플로우 기본)
- 결과를 stdout + 파일(txt/json)로 저장 가능 (워크플로우/레이더 연동용)

의존성:
  pip install pandas numpy yfinance finance-datareader
"""

import argparse
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import yfinance as yf

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
    out = 100 - (100 / (1 + rs))
    return out.fillna(0)


def macd_hist(close: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> pd.Series:
    macd_line = ema(close, fast) - ema(close, slow)
    signal_line = ema(macd_line, signal)
    return macd_line - signal_line


def get_krx_tickers() -> pd.DataFrame:
    if fdr is None:
        raise RuntimeError("FinanceDataReader가 없습니다. requirements.txt에 `finance-datareader`를 넣고 설치하세요.")

    df = fdr.StockListing("KRX")
    if not {"Code", "Name"}.issubset(df.columns):
        raise RuntimeError(f"KRX listing 컬럼이 예상과 다릅니다: {list(df.columns)}")

    if "Market" in df.columns:
        df = df[df["Market"].isin(["KOSPI", "KOSDAQ"])].copy()
        df = df[["Code", "Name", "Market"]].dropna()
        df["Code"] = df["Code"].astype(str).str.zfill(6)
        return df.reset_index(drop=True)

    k1 = fdr.StockListing("KOSPI")[["Code", "Name"]].copy()
    k1["Market"] = "KOSPI"
    k2 = fdr.StockListing("KOSDAQ")[["Code", "Name"]].copy()
    k2["Market"] = "KOSDAQ"
    out = pd.concat([k1, k2], ignore_index=True)
    out["Code"] = out["Code"].astype(str).str.zfill(6)
    return out.reset_index(drop=True)


def yf_symbol_from_code(code: str, market: str) -> str:
    suffix = ".KS" if market == "KOSPI" else ".KQ"
    return f"{str(code).zfill(6)}{suffix}"


def fetch_ohlcv_yf(symbol: str, lookback_days: int = 280) -> pd.DataFrame:
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

    df = df.rename(columns={
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Adj Close": "adj_close",
        "Volume": "volume",
    })
    df.index = pd.to_datetime(df.index)
    return df.sort_index()


def _safe_float(x, default: float = 0.0) -> float:
    """
    pandas/np 값이 Series/ndarray로 들어오는 경우를 방지해서 안전하게 float로 변환
    """
    try:
        if isinstance(x, (pd.Series, np.ndarray, list)):
            if len(x) == 0:
                return default
            x = x[-1]
        if pd.isna(x):
            return default
        return float(x)
    except Exception:
        return default


def compute_signals(df: pd.DataFrame) -> dict:
    """
    ✅ 여기서 Series ambiguous 에러가 났기 때문에
    모든 '마지막 값'을 스칼라로 먼저 만든 후 float 변환
    """
    if df.shape[0] < 70:
        return {}

    close = df["close"].astype(float)
    volume = df["volume"].astype(float)

    ma20 = close.rolling(20).mean()
    rsi14 = rsi(close, 14)
    hist = macd_hist(close, 12, 26, 9)

    # ✅ 핵심 수정: 마지막 변화율을 '먼저' 스칼라로 만든다
    chg_last = (close.pct_change() * 100.0).iloc[-1]

    vol_ma20 = volume.rolling(20).mean()

    last = df.index[-1]
    return {
        "date": last.strftime("%Y-%m-%d"),
        "close": _safe_float(close.iloc[-1]),
        "change_pct": float(chg_last) if pd.notna(chg_last) else 0.0,  # ✅ FIXED
        "ma20": _safe_float(ma20.iloc[-1]),
        "rsi14": _safe_float(rsi14.iloc[-1]),
        "macd_hist": _safe_float(hist.iloc[-1]),
        "volume": _safe_float(volume.iloc[-1]),
        "vol_ma20": _safe_float(vol_ma20.iloc[-1]),
    }


def passes_filters(sig: dict,
                   min_change: float,
                   min_price: float,
                   require_ma20: bool,
                   require_macd: bool,
                   rsi_min: float,
                   use_volume: bool,
                   vol_mult: float) -> bool:
    if not sig:
        return False
    if sig["close"] < min_price:
        return False
    if sig["change_pct"] < min_change:
        return False
    if require_ma20 and not (sig["close"] > sig["ma20"]):
        return False
    if require_macd and not (sig["macd_hist"] >= 0):
        return False
    if sig["rsi14"] < rsi_min:
        return False
    if use_volume:
        if sig["vol_ma20"] <= 0:
            return False
        if sig["volume"] < sig["vol_ma20"] * vol_mult:
            return False
    return True


def format_table(df: pd.DataFrame, use_volume: bool) -> str:
    cols = ["market", "code", "name", "close", "chg%", "ma20", "rsi14", "macd_hist"]
    if use_volume:
        cols += ["vol", "vol_ma20"]
    return df[cols].to_string(index=False)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--min-change", type=float, default=3.0, help="일간 등락률 최소(%) (기본 3.0)")
    ap.add_argument("--min-price", type=float, default=1000.0, help="최소 종가(원) (기본 1000)")
    ap.add_argument("--rsi-min", type=float, default=55.0, help="RSI(14) 최소 (기본 55)")
    ap.add_argument("--top", type=int, default=60, help="상위 N개 출력 (기본 60)")
    ap.add_argument("--use-volume", action="store_true", help="거래량 조건 추가(기본 OFF)")
    ap.add_argument("--vol-mult", type=float, default=1.3, help="거래량 배수(20일평균 대비) (기본 1.3)")
    ap.add_argument("--no-ma20", action="store_true", help="MA20 돌파 조건 끄기")
    ap.add_argument("--no-macd", action="store_true", help="MACD 양수 조건 끄기")
    ap.add_argument("--limit", type=int, default=0, help="테스트용: 티커 N개만(0이면 전체)")

    # ✅ 레이더/워크플로우 연동용 출력 파일
    ap.add_argument("--out-text", type=str, default="out/scan_close_kr.txt", help="텍스트 결과 저장 경로")
    ap.add_argument("--out-json", type=str, default="out/scan_close_kr.json", help="JSON 결과 저장 경로")
    ap.add_argument("--telegram-lines", type=int, default=25, help="텔레그램용 요약 라인 수 (기본 25)")

    args = ap.parse_args()
    require_ma20 = not args.no_ma20
    require_macd = not args.no_macd

    tickers_df = get_krx_tickers()
    if args.limit and args.limit > 0:
        tickers_df = tickers_df.head(args.limit).copy()

    results = []
    total = len(tickers_df)

    for i, row in tickers_df.iterrows():
        code = str(row["Code"]).zfill(6)
        name = str(row["Name"])
        market = str(row["Market"])
        symbol = yf_symbol_from_code(code, market)

        df = fetch_ohlcv_yf(symbol)
        if df.empty:
            continue

        sig = compute_signals(df)
        if not sig:
            continue

        if passes_filters(
            sig=sig,
            min_change=args.min_change,
            min_price=args.min_price,
            require_ma20=require_ma20,
            require_macd=require_macd,
            rsi_min=args.rsi_min,
            use_volume=args.use_volume,
            vol_mult=args.vol_mult,
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

        if (i + 1) % 400 == 0:
            print(f"...progress {i+1}/{total}", file=sys.stderr)

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    header = (
        f"[KR CLOSE SCAN] {now} | min_change={args.min_change}% | rsi>={args.rsi_min} | "
        f"MA20={'ON' if require_ma20 else 'OFF'} | MACD={'ON' if require_macd else 'OFF'} | "
        f"VOL={'ON' if args.use_volume else 'OFF'}"
    )

    out_dir = Path(args.out_text).parent
    out_dir.mkdir(parents=True, exist_ok=True)

    if not results:
        text = header + "\n" + "-" * len(header) + "\nNO SIGNALS\n"
        print(text.strip())
        Path(args.out_text).write_text(text, encoding="utf-8")
        Path(args.out_json).write_text(
            json.dumps({"meta": {"generated_at": now, "header": header, "args": vars(args)}, "items": []},
                       ensure_ascii=False, indent=2),
            encoding="utf-8"
        )
        return

    df_out = pd.DataFrame(results).sort_values(["chg%", "rsi14"], ascending=[False, False]).reset_index(drop=True)
    df_out["close"] = df_out["close"].round(0).astype(int)
    df_out["chg%"] = df_out["chg%"].round(2)
    df_out["ma20"] = df_out["ma20"].round(0).astype(int)
    df_out["rsi14"] = df_out["rsi14"].round(1)
    df_out["macd_hist"] = df_out["macd_hist"].round(4)

    top_n = min(args.top, len(df_out))
    df_top = df_out.head(top_n).copy()

    text = header + "\n" + "-" * len(header) + "\n" + format_table(df_top, args.use_volume) + "\n"
    print(text.strip())

    Path(args.out_text).write_text(text, encoding="utf-8")

    payload = {
        "meta": {
            "generated_at": now,
            "header": header,
            "args": vars(args),
            "count": int(len(df_top)),
        },
        "items": df_top.to_dict(orient="records"),
        "telegram_preview": "\n".join(text.strip().splitlines()[: max(10, args.telegram_lines)]),
    }
    Path(args.out_json).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
