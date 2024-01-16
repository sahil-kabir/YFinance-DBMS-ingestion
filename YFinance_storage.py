import settings
import yfinance as yf
import pandas as pd
import pymysql
from sqlalchemy import create_engine

stock_input = str(input("Please input the stock symbol\n"))
df = yf.download(tickers = stock_input,
	period= "1d",
	interval="1m")

df['Stock'] = stock_input
df.rename(columns={'Adj Close':'Adj_Close'}, inplace=True)
df = df.round(2) # Round all values to 2 decimal places
df.reset_index(level=0, inplace=True) 


conn = pymysql.connect(host=config.setting.host, user=config.setting.db_user, passwd=config.setting.db_password, db = config.setting.db_name)
conn.cursor().execute(
    "CREATE TABLE IF NOT EXISTS Ticks (Datetime TIMESTAMP, Open DECIMAL(4,2), High DECIMAL(4,2), Low DECIMAL(4,2), Close DECIMAL(4,2), Adj_Close DECIMAL(4,2), Volume INT, Stock VARCHAR(5))")
conn.cursor().execute(
    "CREATE TABLE IF NOT EXISTS Stocks (stock VARCHAR(5), latest_date TIMSTAMP, all_high DECIMAL(4,2), all_low DECIMAL(4,2))")

conn = pymysql.connect(host=config.setting.host, user=settings.db_user, passwd=settings.db_password, db = settings.db_name)
cursor = conn.cursor()
cursor.execute(
    "SELECT stock FROM stocks WHERE stock = {}".format(input_stock))
row = cursor.fetchall()

engine = create_engine('mysql+pymysql://{user}:{password}@localhost/{database_name}'.format(
    user = settings.db_user, password = settings.db_password, database_name = settings.db_name))
if not row.empty():
    cursor.execute(
        "UPDATE stocks SET latest_date = {} WHERE stock = {}".format(max(df['Datetime']), input_stock))
    if max(df['high']) > row['all_high']:
        cursor.execute(
        "UPDATE stocks SET all_high = {} WHERE stock = {}".format(max(df['high']), input_stock))
    if min(df['low']) < row['all_low']:
        cursor.execute(
        "UPDATE stocks SET all_low = {} WHERE stock = {}".format(min(df['low']), input_stock))
else: 
    cursor.execute("INSERT INTO stocks (stock, latest_date, all_high, all_low) VALUES ({}, {}, {}, {})".format(
        stocks, max(list(df.index)), max(df['high']), min(df['low'])))
    
df.to_sql(name='{name}'.format(name = "Ticks"), con=engine, if_exists = 'append', index=False)
print("Data has been ingested")