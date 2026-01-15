import pandas as pd
import requests
from io import BytesIO
from zipfile import ZipFile

KRX_URL = "https://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13"

def main():
    # KRX 상장법인 전체 CSV 다운로드 (코스피+코스닥+코넥스 포함)
    df = pd.read_html(KRX_URL, encoding="euc-kr")[0]

    # 종목코드 6자리 보정
    df["종목코드"] = df["종목코드"].astype(str).str.zfill(6)

    lines = []
    for _, r in df.iterrows():
        code = r["종목코드"]
        market = str(r["시장구분"]).strip()

        if market == "KOSPI":
            lines.append(f"{code}.KS")
        elif market == "KOSDAQ":
            lines.append(f"{code}.KQ")

    lines = sorted(set(lines))

    with open("tickers_kr.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

    print("KRX tickers generated:", len(lines))

if __name__ == "__main__":
    main()
