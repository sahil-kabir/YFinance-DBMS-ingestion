import settings
import sys
import yfinance as yf
import pandas as pd
import pymysql
from sqlalchemy import create_engine

class Ingestion():
    def __init__(self, stock_input):
        self.stock_input = stock_input

    @static
    def set_tables():
        conn = pymysql.connect(host=settings.host, user=settings.db_user, passwd=settings.db_password, db=settings.db_name)
        conn.cursor().execute("CREATE TABLE IF NOT EXISTS Ticks (Datetime TIMESTAMP, Open DECIMAL(4,2), High DECIMAL(4,2), Low DECIMAL(4,2), Close DECIMAL(4,2), Adj_Close DECIMAL(4,2), Volume INT, Stock VARCHAR(5))")
        conn.cursor().execute("CREATE TABLE IF NOT EXISTS Stocks (stock VARCHAR(5), latest_date TIMSTAMP, all_high DECIMAL(4,2), all_low DECIMAL(4,2))")

    def ingest(self):
        df = yf.download(tickers = self.stock_input,
                              period= "1d",
                              interval="1m")
        df['Stock'] = stock_input
        df.rename(columns={'Adj Close':'Adj_Close'}, inplace=True)
        df = df.round(2) # Round all values to 2 decimal places
        df.reset_index(level=0, inplace=True)

        self.set_tables()
        
        conn = pymysql.connect(host=config.setting.host, user=settings.db_user, passwd=settings.db_password, db = settings.db_name)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT stock FROM Stocks WHERE stock = {}".format(input_stock))
        row = cursor.fetchall()
        
        engine = create_engine('mysql+pymysql://{user}:{password}@localhost/{database_name}'.format(
            user = settings.db_user, password = settings.db_password, database_name = settings.db_name))
        if not row.empty():
            cursor.execute(
                "UPDATE Stocks SET latest_date = {} WHERE stock = {}".format(max(df['Datetime']), input_stock))
            if max(df['high']) > row['all_high']:
                cursor.execute(
                "UPDATE Stocks SET all_high = {} WHERE stock = {}".format(max(df['high']), input_stock))
            if min(df['low']) < row['all_low']:
                cursor.execute(
                "UPDATE Stocks SET all_low = {} WHERE stock = {}".format(min(df['low']), input_stock))
        else: 
            cursor.execute(
                "INSERT INTO stocks (stock, latest_date, all_high, all_low) VALUES ({}, {}, {}, {})".format(
                stocks, max(list(df.index)), max(df['high']), min(df['low'])))

        df.to_sql(name='{name}'.format(name = "Ticks"), con=engine, if_exists = 'append', index=False)
        print("Data has been ingested")

if __name__ == '__main__':
    ingestion = Ingestion(sys.argv[1])
    ingestion.set_tables()
    ingestion.ingest()