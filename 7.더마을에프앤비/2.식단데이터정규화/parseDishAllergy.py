# parseDishAllergy.py

# 메뉴 데이터 분석 후 DB 저장

import re
import pyodbc
import openai
from dotenv import load_dotenv
import os

load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")

# DB 연결
conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=smarticon.kr,13000;"
    "DATABASE=A2025TheMYFNB;"
    "UID=sa;PWD=gksmfskfk8899"
)
cursor = conn.cursor()


# 알러지 + 메뉴 분석 함수 : 
def parse_dish(text):
    # 1) 알러지 번호 추출
    allergy_match = re.search(r"\(([\d.]+)\)", text)
    allergy_nums = allergy_match.group(1) if allergy_match else ""

    # 2) 특수 태그 추출
    tags = re.findall(r"\[(.*?)\]", text)
    tag_text = ",".join(tags) if tags else ""

    # 3) 메뉴명 정리
    menu = re.sub(r"\(.*?\)|\[.*?\]", "", text)
    menu = menu.strip()

    # 4) AI 보정 (오타·이상한 메뉴명 자동 정제)
    prompt = f"메뉴명 '{menu}' 를 한국어 정상적인 메뉴 표기 형태로 정제해줘."
    res = openai.ChatCompletion.create(
        model="gpt-4o-mini", messages=[{"role": "user", "content": prompt}]
    )
    menu_fixed = res.choices[0].message.content.strip()

    return menu_fixed, allergy_nums, tag_text


# DB에서 dish 데이터 조회
cursor.execute("SELECT id, ddish_nm FROM TBmealInfo_DDISH_NM")
rows = cursor.fetchall()

for row in rows:
    menu, allergy, tag = parse_dish(row.ddish_nm)

    cursor.execute(
        """
        INSERT INTO TBmealInfo_DISH_Parsed (dish_id, menu_name, allergy_nums, tags)
        VALUES (?, ?, ?, ?)
    """,
        row.id,
        menu,
        allergy,
        tag,
    )

conn.commit()
cursor.close()
conn.close()

print("완료! 메뉴 데이터 분석 후 DB 저장했습니다.")
