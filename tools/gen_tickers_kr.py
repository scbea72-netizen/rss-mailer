# tools/gen_tickers_kr.py
import os
import pandas as pd

def main():
    # FinanceDataReader로 KRX 전종목 가져오기
    # (Actions에서 pip install finance-datareader 로 설치됨)
    import FinanceDataReader as fdr

    include_konex = os.getenv("INCLUDE_KONEX", "0").strip() in ("1", "true", "yes")
    out_path = os.getenv("KR_TICKERS_OUT", "tickers_kr.txt")

    df = fdr.StockListing("KRX")
    # 보통 컬럼: Symbol, Name, Market 등(버전에 따라 약간 다를 수 있음)
    # 안전하게 처리
    symbol_col = "Symbol" if "Symbol" in df.columns else ("Code" if "Code" in df.columns else None)
    market_col = "Market" if "Market" in df.columns else None
    if symbol_col is None or market_col is None:
        raise RuntimeError(f"Unexpected columns: {list(df.columns)}")

    df = df[[symbol_col, market_col]].dropna()
    df[symbol_col] = df[symbol_col].astype(str).str.zfill(6)
    df[market_col] = df[market_col].astype(str)

    # KOSPI / KOSDAQ만 기본 포함 (KONEX는 옵션)
    keep = df[df[market_col].isin(["KOSPI", "KOSDAQ"])].copy()
    if include_konex:
        keep = pd.concat([keep, df[df[market_col] == "KONEX"]], ignore_index=True)

    # Yahoo Finance 티커 접미사
    def to_yahoo(code: str, mkt: str) -> str:
        if mkt == "KOSPI":
            return f"{code}.KS"
        if mkt == "KOSDAQ":
            return f"{code}.KQ"
        # KONEX는 Yahoo에서 안정적으로 안 잡히는 경우가 있어 기본 제외
        return None

    tickers = []
    for _, r in keep.iterrows():
        t = to_yahoo(r[symbol_col], r[market_col])
        if t:
            tickers.append(t)

    # 정리: 중복 제거, 정렬
    tickers = sorted(set(tickers))

    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(tickers) + "\n")

    print(f"[OK] wrote {len(tickers)} tickers -> {out_path}")
    print("sample:", tickers[:10])

if __name__ == "__main__":
    main()
