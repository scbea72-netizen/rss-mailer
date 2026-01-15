import yfinance as yf

# ✅ 현실적으로 존재하는 종목코드 구간들(대표 구간)
# - 0xxxxxx, 1xxxxxx, 2xxxxxx, 3xxxxxx, 4xxxxxx, 5xxxxxx, 6xxxxxx, 7xxxxxx, 8xxxxxx, 9xxxxxx
# 전부 찍으면 너무 많으니, "앞 3자리" 단위로 후보를 만들고 검증
PREFIXES = [f"{i:03d}" for i in range(0, 1000)]  # 000~999

def exists(symbol: str) -> bool:
    try:
        t = yf.Ticker(symbol)
        # fast_info가 비어있지 않으면 거의 확실
        fi = getattr(t, "fast_info", None)
        if fi and isinstance(fi, dict) and fi.get("last_price") is not None:
            return True
        # fallback: 1d 히스토리로 존재 확인
        h = t.history(period="5d", interval="1d")
        return h is not None and not h.empty
    except Exception:
        return False

def main():
    kospi = []
    kosdaq = []

    # ✅ 각 prefix마다 대표 후보만 만들어 검증 후, 맞는 대역은 세부 탐색
    # - 이 방식은 "완벽한 100% 전종목"은 아니지만, GitHub에서 안정적으로 돌아가고
    #   실제로 데이터 있는 종목들을 대량으로 잡아냅니다.
    # - 전종목이 꼭 필요하면, 이후에 prefix 범위를 더 촘촘히 확장하면 됩니다.

    for p in PREFIXES:
        # 각 prefix에서 대표 몇 개를 찍어 시장 존재 여부 판단
        candidates = [f"{p}{x:03d}" for x in (0, 100, 200, 300, 400, 500, 600, 700, 800, 900)]
        # 시장별로 둘 다 체크
        ok_any = False
        for code in candidates:
            if exists(f"{code}.KS") or exists(f"{code}.KQ"):
                ok_any = True
                break
        if not ok_any:
            continue

        # 존재하는 prefix면, 그 prefix의 000~999를 스캔(1000개)
        for s in range(1000):
            code = f"{p}{s:03d}"
            ks = f"{code}.KS"
            kq = f"{code}.KQ"
            if exists(ks):
                kospi.append(ks)
            elif exists(kq):
                kosdaq.append(kq)

    lines = sorted(set(kospi + kosdaq))

    if not lines:
        raise RuntimeError("티커 생성 결과 0개 (yfinance 스캔 실패)")

    with open("tickers_kr.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

    print("KR tickers generated:", len(lines))
    print("sample:", lines[:10])

if __name__ == "__main__":
    main()
