from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import pandas as pd
from datetime import datetime, timedelta
import time
from sqlalchemy import create_engine # sql

engine = create_engine('mysql+mysqlconnector://kdn_pv:kdn_pv123@133.186.219.101:3306/kdn_pv')

table_name = 'tbl_weather_forecast'

# 테이블에서 데이터 불러오기
query = f'SELECT * FROM {table_name}'
df = pd.read_sql(query, con=engine)

# 데이터프레임 출력
print(df.head)