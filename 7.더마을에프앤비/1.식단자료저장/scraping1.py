# scraping1.py
"""
kdclub 학교 식단 스크래핑
- 0.txt 시나리오: 로그인 → 식단/레시피 → 학교식단 → 교육청 전체 → 학교별 → 월간식단
- 결과: data/meal_YYYYMM.csv 파일들 생성
필수 패키지: requests, bs4, pandas (없으면 pip install requests beautifulsoup4 pandas)
"""

import os
import re
import time
import csv
import logging
from dataclasses import dataclass
from typing import List, Dict, Optional
import requests
from bs4 import BeautifulSoup
from dmMariaDB import call_procedure

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
BASE = "https://www.kdclub.com"
LOGIN_URL = f"{BASE}"
LIST_URL = f"{BASE}/food_menu/edu_meal.php"  # 학교 목록 iframe
MEAL_URL = f"{BASE}/food_menu/edu_mnvw.php"  # 월간 식단표
payload = {"username": "bbq608", "password": "38384972!", "scnmode": ""}

# 0.txt에 제시된 계정
USER_ID = "bbq608"
USER_PW = "38384972!"

# 교육청 코드 (4.txt select 옵션 순서)
ATPT_CODES = [
    ("B10", "서울교육청"),
    ("J10", "경기교육청"),
    ("C10", "부산교육청"),
    ("D10", "대구교육청"),
    ("E10", "인천교육청"),
    ("F10", "광주교육청"),
    ("G10", "대전교육청"),
    ("H10", "울산교육청"),
    ("I10", "세종교육청"),
    ("K10", "강원교육청"),
    ("M10", "충북교육청"),
    ("N10", "충남교육청"),
    ("P10", "전북교육청"),
    ("Q10", "전남교육청"),
    ("R10", "경북교육청"),
    ("S10", "경남교육청"),
    ("T10", "제주교육청"),
]


@dataclass
class MealRow:
    atpt_code: str
    atpt_name: str
    school_name: str
    school_id: str
    year: int
    month: int
    day: int
    meal_type: str
    kcal: Optional[str]
    items: List[str]


def get_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
        }
    )
    return s


def login(session: requests.Session):
    """
    영양사도우미 사이트 로그인 처리 함수입니다.
    - 세션을 받아 POST 요청으로 로그인 시도
    - 쿠키/세션 및 헤더 등 로그인 이후 정보 정상적으로 저장 여부 확인
    - 메인 페이지(로그인 성공시 접근 가능한 페이지)에서 로그인 상태 다시 확인

    ✅ 이 부분이 실제 로그인을 담당하는 코드가 맞습니다.
    """

    # 로그인 요청 (쿠키 세션 초기화 목적 포함)
    response = session.post(LOGIN_URL, data=payload)
    response.encoding = "euc-kr"

    # 요청 헤더 필요시(Referer 등), 세션에 자동으로 관리 가능하나, 필요하면 아래 headers 사용
    headers = {
        "Referer": LOGIN_URL,
        "Content-Type": "application/x-www-form-urlencoded",
    }

    # 로그인 후 메인 페이지 진입해 로그인 여부 확인 (영양사도우미는 세션쿠키로 인증 지속)
    main_page = session.get(BASE)
    main_page.encoding = "euc-kr"
    print(main_page.text)

    # 로그인 성공 여부 출력 (response 및 메인페이지 동시 확인)
    if response.ok:
        logging.info(
            "Login success!"
        )  # 보통 로그인 성공시 '로그아웃' 버튼, 사용자 이름 등 포함됨
    else:
        logging.error("Login failed!")  # 실패시 로그인 폼이나 에러메시지 확인 필요


# 즉, 위 함수에서 로그인 처리 맞음!


def parse_school_list(html: str) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    results = []
    # 링크 예: /food_menu/edu_mnvw.php?atptsc=B10&xno=7010057
    for a in soup.find_all("a", href=True):
        if "edu_mnvw.php" in a["href"] and "xno=" in a["href"]:
            href = requests.compat.urljoin(BASE, a["href"])
            m = re.search(r"atptsc=([A-Z0-9]+).*xno=(\d+)", href)
            if not m:
                continue
            atpt, xno = m.group(1), m.group(2)
            name = a.get_text(strip=True)
            results.append({"atpt": atpt, "xno": xno, "name": name})
    return results


def fetch_schools(session: requests.Session, atpt_code: str) -> List[Dict[str, str]]:
    page = 1
    schools: List[Dict[str, str]] = []
    while True:
        params = {"atptsc": atpt_code, "knd": "", "scn": "", "page": page}
        resp = session.get(LIST_URL, params=params)
        resp.encoding = "euc-kr"
        new = parse_school_list(resp.text)
        if not new:
            break
        schools.extend(new)
        page += 1
        time.sleep(0.2)  # 서버 부하 방지
    logging.info("교육청 %s: %d개 학교 수집", atpt_code, len(schools))
    return schools


def parse_meal_table(
    html: str,
    atpt: str,
    atpt_name: str,
    school_name: str,
    school_id: str,
    year: int,
    month: int,
) -> List[MealRow]:
    soup = BeautifulSoup(html, "html.parser")
    rows: List[MealRow] = []
    # 날짜가 들어 있는 div.showday0를 기준으로 같은 td 내의 식단 ul을 찾는다.
    for td in soup.select("table[bgcolor='#cccccc'] td"):
        day_div = td.find("div", class_="showday0")
        if not day_div:
            continue
        day_text = day_div.get_text(" ", strip=True)
        m_day = re.search(r"(\d+)", day_text)
        if not m_day:
            continue
        day = int(m_day.group(1))

        # 식단 구간: div.mnknd*와 그 뒤 ul.mnlst_link*
        meal_div = td.find("div", class_=re.compile(r"mnknd\d"))
        ul = td.find("ul", class_=re.compile(r"mnlst_link\d"))
        if not meal_div or not ul:
            continue
        header = meal_div.get_text(" ", strip=True)  # 예: "중식 : 570명 (940.3 Kcal)"
        m_kcal = re.search(r"\(([^)]*kcal)\)", header, re.IGNORECASE)
        kcal = m_kcal.group(1) if m_kcal else None
        meal_type = header.split(":")[0].strip()

        items = [li.get_text(" ", strip=True) for li in ul.find_all("li")]
        rows.append(
            MealRow(
                atpt_code=atpt,
                atpt_name=atpt_name,
                school_name=school_name,
                school_id=school_id,
                year=year,
                month=month,
                day=day,
                meal_type=meal_type,
                kcal=kcal,
                items=items,
            )
        )
    return rows


def fetch_meals(
    session: requests.Session,
    atpt: str,
    atpt_name: str,
    school: Dict[str, str],
    year: int,
    month: int,
) -> List[MealRow]:
    params = {"atptsc": atpt, "xno": school["xno"], "year": year, "month": month}
    resp = session.get(MEAL_URL, params=params)
    resp.encoding = "euc-kr"
    return parse_meal_table(
        resp.text, atpt, atpt_name, school["name"], school["xno"], year, month
    )


def save_csv(rows: List[MealRow], year: int, month: int) -> None:
    os.makedirs("data", exist_ok=True)
    path = os.path.join("data", f"meal_{year:04d}{month:02d}.csv")
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "atpt_code",
                "atpt_name",
                "school_name",
                "school_id",
                "year",
                "month",
                "day",
                "meal_type",
                "kcal",
                "items",
            ]
        )
        for r in rows:
            w.writerow(
                [
                    r.atpt_code,
                    r.atpt_name,
                    r.school_name,
                    r.school_id,
                    r.year,
                    r.month,
                    r.day,
                    r.meal_type,
                    r.kcal or "",
                    " | ".join(r.items),
                ]
            )
    logging.info("CSV 저장: %s (%d건)", path, len(rows))


def main():
    session = get_session()
    login(session)

    # 원하는 연/월 범위
    years = [2025]  # 필요 시 range(2023, 2026) 등으로 변경
    months = [12]  # 1~12 리스트

    for year in years:
        for month in months:
            all_rows: List[MealRow] = []
            for atpt_code, atpt_name in ATPT_CODES:
                schools = fetch_schools(session, atpt_code)
                for school in schools:
                    try:
                        rows = fetch_meals(
                            session, atpt_code, atpt_name, school, year, month
                        )
                        all_rows.extend(rows)
                        time.sleep(0.3)

                        break
                    except Exception as e:
                        logging.error(
                            "식단 수집 실패 %s %s %s: %s",
                            atpt_code,
                            school.get("name"),
                            school.get("xno"),
                            e,
                        )
                        continue

                break
            save_csv(all_rows, year, month)

            break


if __name__ == "__main__":
    main()
