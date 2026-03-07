# crawing.py
# Using requests + Session (Most common for normal web login)

import requests

# # 더마을에프앤비 MES
# LOGIN_URL = "http://smarticon.kr:25103/login"
# TARGET_URL = "http://smarticon.kr:25103/e10_TBCode"
# payload = {"username": "admin", "password": "1234"}

# 영양사도우미
LOGIN_URL = "https://www.kdclub.com/"
TARGET_URL = "https://www.kdclub.com/food_menu/edu_meal.php"
payload = {"username": "bbq608", "password": "38384972!"}


def login(session: requests.Session, payload: dict):
    response = session.post(LOGIN_URL, data=payload)
    if response.ok:
        print("Login success!")
    else:
        print("Login failed!")

    mypage = session.get(TARGET_URL)
    print(mypage.text)


def main():
    session = requests.Session()
    login(session, payload)


if __name__ == "__main__":
    main()
