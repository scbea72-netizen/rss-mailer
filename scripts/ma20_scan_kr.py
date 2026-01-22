from __future__ import annotations

import os
from pathlib import Path

import pandas as pd

from utils_krx import recent_trading_days, fetch_bulk_ohlcv_for_date
from notify import send_email, send_telegram

CACHE_DIR = Path(".cache")
CACHE_DIR.mkdir(exist_ok=True)


def load_or_fetch_history(trading_days: list[str]) -> pd.DataFrame:
    """
    ✅ 핵심 개선점
    - 특정 날짜에서 pykrx/krx 차단/장애가 떠도 '전체 잡'을 죽이지 않음
    - 실패 날짜는 스킵하고 계속 진행
    """
    frames = []
    ok_days = []

    for d in trading_days:
        fp = CACHE_DIR / f"krx_{d}.csv"

        try:
            if fp.exists():
                df = pd.read_csv(fp, dtype={"ticker": str, "name": str, "date": str, "market": str})
                if df is None or df.empty:
                    raise RuntimeError("cache empty")
            else:
                df = fetch_bulk_ohlcv_for_date(d)
                df.to_csv(fp, index=False, encoding="utf-8-sig")

            frames.append(df)
            ok_days.append(d)

        except Exception as e:
            print(f"[WARN] skip day {d} due to fetch/cache error: {e}")
            continue

    if not frames:
        raise RuntimeError("히스토리 데이터 수집에 완전히 실패했습니다. (KRX 차단/장애 가능)")

    hist = pd.concat(frames, ignore_index=True)
    hist["date"] = hist["date"].astype(str)

    # 실제 확보된 날짜 기준으로 정렬
    got_dates = sorted(hist["date"].unique())
    print(f"[INFO] fetched_days={got_dates[0]}..{got_dates[-1]} ({len(got_dates)}) / requested={len(trading_days)}")

    return hist


def compute_signals(hist: pd.DataFrame, value_floor: int, near_pct: float):
    dates = sorted(hist["date"].unique())
    if len(dates) < 21:
        raise RuntimeError(f"거래일이 부족합니다. 필요>=21, 현재={len(dates)} (KRX 차단/장애로 일부 날짜 스킵 가능)")

    prev_date = dates[-2]
    latest_date = dates[-1]

    close_pv = hist.pivot_table(index="date", columns="ticker", values="close", aggfunc="last").sort_index()
    value_pv = hist.pivot_table(index="date", columns="ticker", values="value", aggfunc="last").sort_index()

    ma20 = close_pv.rolling(window=20, min_periods=20).mean()

    c_prev = close_pv.loc[prev_date]
    c_now  = close_pv.loc[latest_date]
    m_prev = ma20.loc[prev_date]
    m_now  = ma20.loc[latest_date]
    v_now  = value_pv.loc[latest_date]

    liquid = v_now >= value_floor

    breakout_mask = (c_prev <= m_prev) & (c_now > m_now) & liquid
    near_mask = ((c_now - m_now).abs() / m_now <= near_pct) & liquid

    tickers = close_pv.columns.tolist()

    name_map = (
        hist[hist["date"] == latest_date][["ticker", "name"]]
        .drop_duplicates(subset=["ticker"])
        .set_index("ticker")["name"]
        .to_dict()
    )

    def build_df(mask) -> pd.DataFrame:
        sel = [t for t in tickers if bool(mask.get(t, False))]
        if not sel:
            return pd.DataFrame(columns=["ticker", "name", "close", "ma20", "gap_pct", "value"])

        df = pd.DataFrame({
            "ticker": sel,
            "name": [name_map.get(t, "") for t in sel],
            "close": c_now.loc[sel].values,
            "ma20": m_now.loc[sel].values,
            "gap_pct": ((c_now.loc[sel] - m_now.loc[sel]) / m_now.loc[sel] * 100.0).values,
            "value": v_now.loc[sel].values,
        }).sort_values("value", ascending=False)

        df["close"] = df["close"].round(0).astype("int64")
        df["ma20"] = df["ma20"].round(1)
        df["gap_pct"] = df["gap_pct"].round(2)
        df["value"] = df["value"].round(0).astype("int64")
        return df

    return build_df(breakout_mask), build_df(near_mask), latest_date, prev_date


def fmt_table(df: pd.DataFrame, limit: int) -> str:
    if df.empty:
        return "- (없음)"
    d = df.head(limit).copy()
    d["value_억"] = (d["value"] / 1e8).round(1)
    cols = ["ticker", "name", "close", "ma20", "gap_pct", "value_억"]
    return d[cols].to_string(index=False)


def main() -> None:
    top_n = int(os.environ.get("TOP_N", "50"))
    value_floor = int(os.environ.get("VALUE_FLOOR", str(5_000_000_000)))  # 50억
    near_pct = float(os.environ.get("NEAR_PCT", "0.01"))  # 1%

    # ✅ 개선: KRX/pykrx가 특정 날짜에서 실패할 수 있으니 넉넉히 더 뽑아서(예: 45개)
    # 스킵이 발생해도 최종 25개 이상 확보되게 함.
    target_days = int(os.environ.get("TRADING_DAYS", "25"))
    fetch_days = int(os.environ.get("FETCH_DAYS", str(max(45, target_days + 20))))
    end_date = os.environ.get("END_DATE")  # 옵션: YYYYMMDD 또는 YYYY-MM-DD

    days = recent_trading_days(fetch_days, end_date=end_date)
    day_list = [d.yyyymmdd for d in days]
    print(f"[INFO] requested_trading_days={day_list[0]}..{day_list[-1]} ({len(day_list)}) end_date={end_date or '(auto-kst)'}")

    hist = load_or_fetch_history(day_list)

    # ✅ 실제 확보된 날짜 중에서 최신 target_days만 사용
    got_dates = sorted(hist["date"].unique())
    if len(got_dates) < target_days:
        raise RuntimeError(f"확보된 거래일이 부족합니다. 필요={target_days}, 확보={len(got_dates)}")

    use_dates = got_dates[-target_days:]
    hist = hist[hist["date"].isin(use_dates)].copy()

    print(f"[INFO] using_trading_days={use_dates[0]}..{use_dates[-1]} ({len(use_dates)})")

    breakouts, near, latest_date, prev_date = compute_signals(hist, value_floor=value_floor, near_pct=near_pct)

    subject = f"[KR] MA20 종가 시그널 ({latest_date})"
    body = [
        f"기준일: {latest_date} (전일: {prev_date})",
        f"필터: 거래대금 ≥ {value_floor/1e8:.0f}억, 근접범위 ±{near_pct*100:.1f}%",
        "",
        "1) ✅ MA20 종가 돌파 (전일≤MA20 & 금일>MA20)",
        fmt_table(breakouts, top_n),
        "",
        "2) 👀 MA20 근접 (±범위 이내)",
        fmt_table(near, top_n),
    ]
    body_text = "\n".join(body)

    tg_lines = [
        f"📌 [KR] MA20 종가 시그널 {latest_date}",
        f"필터: 거래대금≥{value_floor/1e8:.0f}억 / 근접±{near_pct*100:.1f}%",
        "",
        "✅ 돌파 TOP",
    ]
    if breakouts.empty:
        tg_lines.append("(없음)")
    else:
        for _, r in breakouts.head(top_n).iterrows():
            tg_lines.append(
                f"{r['ticker']} {r['name']} | 종가 {int(r['close'])} | MA20 {r['ma20']} | {r['gap_pct']}% | {r['value']/1e8:.1f}억"
            )

    tg_lines += ["", "👀 근접 TOP"]
    if near.empty:
        tg_lines.append("(없음)")
    else:
        for _, r in near.head(top_n).iterrows():
            tg_lines.append(
                f"{r['ticker']} {r['name']} | 종가 {int(r['close'])} | MA20 {r['ma20']} | {r['gap_pct']}% | {r['value']/1e8:.1f}억"
            )

    tg_text = "\n".join(tg_lines)

    send_email(subject, body_text)
    send_telegram(tg_text)

    print("\n" + body_text)


if __name__ == "__main__":
    main()

