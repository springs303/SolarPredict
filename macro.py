from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import pandas as pd
from datetime import datetime, timedelta
import time
from sqlalchemy import create_engine # sql

today = datetime.now().date()

print(today, "macro 실행")

engine = create_engine('mysql+mysqlconnector://kdn_pv:kdn_pv123@133.186.219.101:3306/kdn_pv')

options = webdriver.ChromeOptions()
# 창 숨기는 옵션 추가
options.add_argument("headless")

driver = webdriver.Chrome(options=options)
# 웹페이지 해당 주소 이동
driver.get("https://bd.kma.go.kr/kma2020/fs/energySelect1.do?pageNum=5&menuCd=F050701000")
driver.maximize_window()
time.sleep(5)

def init_data():
    data = {
      "local":[],
      "time": [],
      "today_power": [],
      "today_power_sum": [],
      "today_solar": [],
      "today_temper": [],
      "today_wind": [],
      "tomorrow_power": [],
      "tomorrow_power_sum": [],
      "tomorrow_solar": [],
      "tomorrow_temper": [],
      "tomorrow_wind": []
    }
    df = pd.DataFrame(data)
    df = df.astype({time:'datetime64[s]'})
    return data, df

def append_data(data, tr_elem, local):
  data = dict(data)
  for idx, tr in enumerate(tr_elem):
    if idx in [0,1]: # 컬럼명은 생략
      continue
    tr_split = str(tr.text).split()
    today = datetime.now().date()
    time = datetime.combine(today, datetime.min.time()) + timedelta(hours=int(tr_split[0][:-1]))
    data["time"].append(str(time))  # 시간
    data["local"].append(local)
    data["today_power"].append(tr_split[1])  # 발전량(Mw)
    data["today_power_sum"].append(tr_split[2]) # 누적발전량(Mw)
    data["today_solar"].append(tr_split[3]) # 일사량(W/㎡)
    data["today_temper"].append(tr_split[4]) # 기온(℃)
    data["today_wind"].append(tr_split[5]) # 풍속(㎧)
    data["tomorrow_power"].append(tr_split[6])  # 발전량(Mw)
    data["tomorrow_power_sum"].append(tr_split[7]) # 누적발전량(Mw)
    data["tomorrow_solar"].append(tr_split[8]) # 일사량(W/㎡)
    data["tomorrow_temper"].append(tr_split[9]) # 기온(℃)
    data["tomorrow_wind"].append(tr_split[10]) # 풍속(㎧)
  return data

def collect_dataframe(local, tr_elem, df):
  data, _ = init_data()
  data = append_data(data, tr_elem, local)
  df2 = pd.DataFrame(data)
  df = pd.concat([df,df2])
  return df

def save_data(df):
  # csv로 저장(백업용)
  df.to_csv(f'solar_pred_{today}.csv', index=False, mode="w")

  # 데이터프레임을 MariaDB에 쓰기
  df.to_sql('tbl_weather_forecast', con=engine, if_exists='append', index=False, method='multi')
  # 'if_exists' 매개변수는 'fail' (기본값), 'replace', 'append' 중 하나를 선택할 수 있습니다.
  # 'fail': 테이블이 존재할 경우 아무 작업도 수행하지 않습니다.
  # 'replace': 테이블을 삭제하고 새로운 데이터프레임을 씁니다.
  # 'append': 테이블에 데이터프레임을 추가합니다.


# 크롤링 불가능지역(발전설비없음) : 강원특별자치도, 충청북도
# 크롤링 가능지역
locals = ["서울특별시","인천광역시", "경기도", "충청남도", "세종특별자치시", "대전광역시", "전라북도","전라남도","광주광역시","경상남도","경상북도","대구광역시","울산광역시","부산광역시","제주특별자치도"]
_, df = init_data()
for local in locals:
   print(local)
   item = driver.find_element(By.XPATH , f'//*[ text() = "{local}" ]')
   # click이 안되서 우회 명령어
   driver.execute_script("arguments[0].click();", item)
   time.sleep(5)
   # 예보 테이블 검색
   table = driver.find_element(By.CSS_SELECTOR,'.energy_tbl')
   tr_elem = table.find_elements(By.TAG_NAME, 'tr')
   df = collect_dataframe(local, tr_elem, df)
   save_data(df)

# driver 종료
driver.quit()