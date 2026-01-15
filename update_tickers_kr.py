import pandas as pd
import zipfile, io, requests

CSV_ZIP = "https://file.krx.co.kr/download.jspx?filetype=csv&url=CORPCODE.zip"

def main():
    r = requests.get(CSV_ZIP, timeout=30)
    if r.status_code != 200:
        raise RuntimeError("KRX CSV 다운로드 실패")

    z = zipfile.ZipFile(io.BytesIO(r.content))
    name = z.namelist()[0]
    df = pd.read_csv(z.open(name), encoding="euc-kr")

    df["종목코드"] = df["종목코드"].astype(str).str.zfill(6)
    df["시장구분"] = df["시장구분"].astype(str).str.strip()

    lines = []
    for _, r in df.iterrows():
        if r["시장구분"] in ("KOSPI", "유가증권"):
            lines.append(f"{r['종목코드']}.KS")
        elif r["시장구분"] in ("KOSDAQ", "코스닥"):
            lines.append(f"{r['종목코드']}.KQ")

    if not lines:
        raise RuntimeError("티커 생성 결과 0개")

    with open("tickers_kr.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(lines)))

    print("KRX tickers generated:", len(lines))

if __name__ == "__main__":
    main()
