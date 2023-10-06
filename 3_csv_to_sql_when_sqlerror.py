from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import pandas as pd
from datetime import datetime, timedelta
import time
from sqlalchemy import create_engine # sql

engine = create_engine('mysql+mysqlconnector://kdn_pv:kdn_pv123@133.186.219.101:3306/kdn_pv')

# df = pd.read_csv(f'solar_pred_{datetime.now().date()}.csv')
df = pd.read_csv(f'solar_pred_2023-10-05.csv')
df.to_sql('tbl_weather_forecast', con=engine, if_exists='append', index=False)
