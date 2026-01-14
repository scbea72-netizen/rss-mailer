import os, sys, re
from openai import OpenAI

client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])

def clean(s):
    return re.sub(r"\r\n", "\n", s).strip()

def main():
    raw = sys.stdin.read()
    if not raw.strip():
        print("번역할 내용이 없습니다.")
        return

    prompt = f"""
너는 투자자를 위한 '뉴스 한글 브리핑 편집자'다.

요구:
- 아래 RSS Digest 원문을 한국어로 자연스럽게 번역
- 맨 위에 '오늘의 핵심 5줄'과 '투자 영향(수혜/리스크)' 추가
- 링크 유지
- 과장 금지

[원문]
{raw}
"""

    resp = client.chat.completions.create(
        model="gpt-4.1-mini",
        messages=[
            {"role":"system","content":"You translate and summarize economy/market news to Korean."},
            {"role":"user","content":prompt}
        ],
        temperature=0.2
    )
    print(resp.choices[0].message.content.strip())

if __name__ == "__main__":
    main()
