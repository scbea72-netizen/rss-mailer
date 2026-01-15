import pandas as pd

KRX_URL = "https://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13"

def main():
    # KRX 상장법인 리스트(코스피/코스닥/코넥스 포함)
    tables = pd.read_html(KRX_URL, encoding="euc-kr")
    if not tables:
        raise RuntimeError("KRX에서 테이블을 읽지 못했습니다 (read_html 결과 0).")

    df = tables[0].copy()

    # 컬럼명 안전 처리
    if "종목코드" not in df.columns or "시장구분" not in df.columns:
        raise RuntimeError(f"예상 컬럼이 없습니다. columns={list(df.columns)}")

    df["종목코드"] = df["종목코드"].astype(str).str.zfill(6)
    df["시장구분"] = df["시장구분"].astype(str).str.strip()

    lines = []
    for _, r in df.iterrows():
        code = r["종목코드"]
        market = r["시장구분"]

        # ✅ KRX가 영문/한글 둘 다 내려오는 케이스 대응
        if market in ("KOSPI", "유가증권"):
            lines.append(f"{code}.KS")
        elif market in ("KOSDAQ", "코스닥"):
            lines.append(f"{code}.KQ")

    lines = sorted(set(lines))

    # ✅ 0개면 바로 실패시켜서(=Actions 빨간불) 원인 숨김 방지
    if len(lines) == 0:
        sample = df["시장구분"].value_counts().head(10).to_dict()
        raise RuntimeError(f"티커가 0개입니다. 시장구분 샘플={sample}")

    with open("tickers_kr.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

    print(f"[OK] tickers_kr.txt generated: {len(lines)}")
    print("sample:", lines[:10])

if __name__ == "__main__":
    main()
