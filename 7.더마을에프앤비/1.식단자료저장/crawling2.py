# crawling2.py
# If the login uses AJAX / JavaScript → Selenium 필요

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import time

driver = webdriver.Chrome()


LOGIN_URL = "http://smarticon.kr:25103/login"
TARGET_URL = "http://smarticon.kr:25103/e10_TBCode"

driver.get(LOGIN_URL)

# ID 입력
driver.find_element(By.ID, "loginId_t").send_keys("admin")

# PW 입력
driver.find_element(By.ID, "password_t").send_keys("1234")

# 로그인 버튼 클릭
# 클래스 선택자임(여기서는 button 태그에 login이라는 class 사용)
driver.find_element(By.CLASS_NAME, "login_btn").click()  # 클래스명으로도 가능
# driver.find_element(By.ID, "loginBtn").click()      # 아이디가 있다면 아이디로도 가능
# driver.find_element(By.CSS_SELECTOR, "button.login").click()  # 이 코드는 클래스 선택자(css selector) 방식임

time.sleep(3)

# 로그인 후 페이지 접근
driver.get(TARGET_URL)

print(driver.page_source)

driver.quit()
