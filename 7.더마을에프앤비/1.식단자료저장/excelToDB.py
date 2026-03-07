# execlToSchedule.py : 나이스 교육정보 개발포털의 학교기본정보를 기준으로 각 학교별 식단표를 추출하는 프로그램
import datetime
import json
import os
import time
import xml.etree.ElementTree as ET

import pandas as pd
import requests
from dmMssql import call_procedure


# 함수 : 학교기본정보 엑셀파일을 연다
def read_excel(file_path):
    df = pd.read_excel(file_path)
    return df


# 함수 : 학교기본정보 엑셀파일로 부터 시도교육청코드와 행장표준코드를 모두 읽는다.
def read_school_info(df):
    school_info = df[
        ["시도교육청코드", "시도교육청명", "행정표준코드", "학교명", "학교종류명"]
    ]
    return school_info


# 함수 : 나이스 교육정보 개발포털의 API를 호출하여 식단표를 추출한다.
def call_api(atpt_code, xno, MLSV_FROM_YMD, MLSV_TO_YMD):
    url = f"https://open.neis.go.kr/hub/mealServiceDietInfo?KEY=3104c13cd23c4145859ada79db6b8378&ATPT_OFCDC_SC_CODE={atpt_code}&SD_SCHUL_CODE={xno}&MLSV_FROM_YMD={MLSV_FROM_YMD}&MLSV_TO_YMD={MLSV_TO_YMD}"
    response = requests.get(url)

    # response.text를 json 변환

    # Open NEIS API returns XML. We want to parse this as a Python data structure (dictionary/list).
    if response.status_code == 200:
        try:
            root = ET.fromstring(response.text)
            rows = []
            for row in root.iter("row"):
                # Each field (by column number above). Set default to empty string if not found.
                field = lambda tag: (
                    row.find(tag).text.strip()
                    if row.find(tag) is not None and row.find(tag).text
                    else ""
                )
                row_data = {
                    "ATPT_OFCDC_SC_CODE": field("ATPT_OFCDC_SC_CODE"),
                    "ATPT_OFCDC_SC_NM": field("ATPT_OFCDC_SC_NM"),
                    "SD_SCHUL_CODE": field("SD_SCHUL_CODE"),
                    "SCHUL_NM": field("SCHUL_NM"),
                    "MMEAL_SC_CODE": field("MMEAL_SC_CODE"),
                    "MMEAL_SC_NM": field("MMEAL_SC_NM"),
                    "MLSV_YMD": field("MLSV_YMD"),
                    "MLSV_FGR": field("MLSV_FGR"),
                    "DDISH_NM": field("DDISH_NM"),
                    "ORPLC_INFO": field("ORPLC_INFO"),
                    "CAL_INFO": field("CAL_INFO"),
                    "NTR_INFO": field("NTR_INFO"),
                    "MLSV_FROM_YMD": field("MLSV_FROM_YMD"),
                    "MLSV_TO_YMD": field("MLSV_TO_YMD"),
                    "LOAD_DTM": field("LOAD_DTM"),
                }
                rows.append(row_data)
            return rows
        except Exception as e:
            print("XML 파싱 중 오류 발생:", e)
            return None
    else:
        print("API 요청 실패:", response.status_code)
        return None
    return rows


# 함수 : 모든 학교정보에 대해 한개씩 식단표를 추출한다.
def extract_schedule(school_info):
    for index, row in school_info.iterrows():
        atpt_code = row["시도교육청코드"]
        xno = row["행정표준코드"]
        # 2021년 부터 2025년 까지 1월 1일부터 11월 30일까지 식단표를 추출한다.
        for year in range(2025, 2020, -1):
            for month in range(1, 13):
                MLSV_FROM_YMD = f"{year}{month:02d}01"
                # 각 달의 마지막 날을 계산
                if month == 12:
                    MLSV_TO_YMD = f"{year}1231"
                else:
                    # 다음 달의 첫 날을 계산
                    next_month = month + 1
                    next_month_first = datetime.date(year, next_month, 1)
                    last_day = (next_month_first - datetime.timedelta(days=1)).day
                    MLSV_TO_YMD = f"{year}{month:02d}{last_day:02d}"

                rows = call_api(atpt_code, xno, MLSV_FROM_YMD, MLSV_TO_YMD)
                if rows:
                    # row 의 크기만큼 반복하면서 테이블에 저장한다.
                    for row in rows:
                        # 프로시저는 15개의 개별 파라미터를 받으므로 딕셔너리에서 값을 추출하여 튜플로 변환
                        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        print("---------------------------------------")
                        print(
                            {now},
                            "/",
                            row.get("ATPT_OFCDC_SC_NM", ""),
                            "/",
                            row.get("SCHUL_NM", ""),
                            "/",
                            row.get("MLSV_YMD", ""),
                        )
                        print("---------------------------------------")
                        print(str(row))
                        params = (
                            row.get("ATPT_OFCDC_SC_CODE", ""),
                            row.get("ATPT_OFCDC_SC_NM", ""),
                            row.get("SD_SCHUL_CODE", ""),
                            row.get("SCHUL_NM", ""),
                            row.get("MMEAL_SC_CODE", ""),
                            row.get("MMEAL_SC_NM", ""),
                            row.get("MLSV_YMD", ""),
                            row.get("MLSV_FGR", ""),
                            row.get("DDISH_NM", ""),
                            row.get("ORPLC_INFO", ""),
                            row.get("CAL_INFO", ""),
                            row.get("NTR_INFO", ""),
                            row.get("MLSV_FROM_YMD", ""),
                            row.get("MLSV_TO_YMD", ""),
                            row.get("LOAD_DTM", ""),
                        )
                        call_procedure("USP_TBMealInfo_INSERT", params)
                time.sleep(2)


# 메인함수
def main():
    # 학교기본정보 엑셀 열기
    file_path = os.path.join(os.path.dirname(__file__), "data", "학교기본정보.xlsx")
    df = read_excel(file_path)
    # 학교기본정보 엑셀 파일로 부터 시도교육청코드와 행장표준코드를 모두 읽는다.
    school_info = read_school_info(df)
    # 모든 학교정보에 대해 한개씩 식단표를 추출하고 테이블에 저장한다.
    extract_schedule(school_info)


if __name__ == "__main__":
    main()
