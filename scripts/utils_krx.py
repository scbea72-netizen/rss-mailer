from __future__ import annotations

import os
import time
import requests
import pandas as pd
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import List

# ===== KIS ENV =====
KIS_APPKEY = os.environ.get("KIS_APPKEY")
KIS_APPSECRET = os.environ.get("KIS_APPSECRET")
KIS_BASE_URL = os.environ.get("KIS_BASE_URL")  # https://openapi.koreainvestment.com:9443

KOSPI_URL = os.environ.get("KIS_KOSPI_MST_URL")
KOSDAQ_URL = os.environ.get("KIS_KOSDAQ_MST_URL")


@dataclass(frozen=True)
class TradingDay:
    yyyymmdd: str


# ---------- KIS AUTH ----------
def _get_access_token() -> str:
    url = f"{KIS_BASE_URL}/oauth2/tokenP"
    headers = {"content-type": "application/json"}
    body = {
        "grant_type": "client_credentials",
        "appkey": KIS_APPKEY,
        "appsecret": KIS_APPSECRET,
    }
    r = requests.post(url, headers=headers, json=body, timeout=10)
    r.raise_for_status()
    return r.json()["access_token"]


# ---------- TRADING DAYS ----------
def recent_trading_days(n: int, end_date: str | None = None) -> List[TradingDay]:
    """
    KRX 조회 제거.
    최근 '평일' 기준으로만 계산 (휴장일은 이후 OHLCV에서 자동 제외)
    """
    if not end_date:
        now = datetime.now()
        if now.hour < 16:
            d = now.date() - timedelta(days=1)
        else:
            d = now.date()
    else:
        d = datetime.strptime(end_date, "%Y%m%d").date()

    days = []
    cur = d
    while len(days) < n:
        if cur.weekday() < 5:
            days.append(cur.strftime("%Y%m%d"))
        cur -= timedelta(days=1)

    return list(reversed([TradingDay(x) for x in days]))


# ---------- MASTER ----------
def _load_master(url: str) -> pd.DataFrame:
    df = pd.read_csv(url, sep="|", encoding="cp949")
    return df[["단축코드", "한글종목명"]].rename(
        columns={"단축코드": "ticker", "한글종목명": "name"}
    )


# ---------- OHLCV ----------
def fetch_bulk_ohlcv_for_date(date_yyyymmdd: str) -> pd.DataFrame:
    """
    ✅ KIS 기반 OHLCV 수집 (GitHub Actions 안정)
    """
    token = _get_access_token()
    headers = {
        "authorization": f"Bearer {token}",
        "appkey": KIS_APPKEY,
        "appsecret": KIS_APPSECRET,
        "tr_id": "FHKST01010100",
    }

    frames = []

    for market, master_url in [
        ("KOSPI", KOSPI_URL),
        ("KOSDAQ", KOSDAQ_URL),
    ]:
        master = _load_master(master_url)

        rows = []
        for _, r in master.iterrows():
            params = {
                "fid_cond_mrkt_div_code": "J",
                "fid_input_iscd": r["ticker"],
                "fid_input_date_1": date_yyyymmdd,
                "fid_input_date_2": date_yyyymmdd,
                "fid_period_div_code": "D",
                "fid_org_adj_prc": "1",
            }

            try:
                resp = requests.get(
                    f"{KIS_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
                    headers=headers,
                    params=params,
                    timeout=5,
                )
                data = resp.json()
                out = data.get("output2")
                if not out:
                    continue

                o = out[0]
                rows.append(
                    {
                        "ticker": r["ticker"],
                        "name": r["name"],
                        "close": int(o["stck_clpr"]),
                        "value": int(o["acml_tr_pbmn"]),
                        "date": date_yyyymmdd,
                        "market": market,
                    }
                )
            except Exception:
                continue

        if rows:
            frames.append(pd.DataFrame(rows))

        time.sleep(0.3)  # API 보호

    if not frames:
        raise RuntimeError(f"{date_yyyymmdd} 데이터 수집 실패(KIS)")

    return pd.concat(frames, ignore_index=True)
