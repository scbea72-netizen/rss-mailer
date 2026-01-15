import pandas as pd

KRX_URL = "https://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13"

def main():
    df = pd.read_html(KRX_URL, encoding="euc-kr")[0]

    df["종목코드"] = df["종목코드"].astype(str).str.zfill(6)

    lines = []
    for _, r in df.iterrows():
        market = str(r["시장구분"]).strip()
        code = r["종목코드"]

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
