from dotenv import load_dotenv
import os
import pandas as pd
import sys
from datetime import datetime
from sqlalchemy import create_engine
import pyodbc

# minIO
from minio import Minio
from io import BytesIO

# load variables
load_dotenv()

# create connect sql
def init_db_connect():
    try:
        SERVER = os.environ.get("SERVER")
        DATABASE = os.environ.get("DATABASE")
        DRIVER = os.environ.get("DRIVER")
        USERNAME = os.environ.get("USERNAME_LOGIN")
        PASSWORD = os.environ.get("PASSWORD")
        DATABASE_CONNECTION = f'mssql+pyodbc://{USERNAME}:{PASSWORD}@{SERVER}/{DATABASE}?driver={DRIVER}'

        engine = create_engine(DATABASE_CONNECTION)
        return engine
    except Exception as e:
        print(e)
        raise Exception('xxxxxxxxxx')

# run query sql
def query_result():
    try:
        query =  "Conforme capítulo 02"
        df = pd.read_sql(query,init_db_connect())
        df.head(3)
        return df
    except Exception as e:
        print(e)
        raise Exception('xxxxxxxx')

# create connect minIO
def init_minio_connect():
    try:
        client = Minio(os.environ.get("MINIO"), os.environ.get("ACCESS_KEY"), os.environ.get("SECRET_KEY"), secure=False)
        return client
    except Exception as e:
        print(e)
        raise Exception('xxxxxxxxx')

# send data to minIO
def send_data_to_minio(client,df):
    try:
        # set year month day
        year = datetime.today().year
        month = datetime.today().month
        day = datetime.today().day

        # set path in bucket
        empresa = ['empresa']
        entity = 'lgpd'

        # export to csv format
        # set records and utf-8
        # create file into minio location
        csv_data = df.to_csv(sep=',',index=False).encode('utf-8')
        csv_buffer = BytesIO(csv_data)
        name = f'{empresa}/{entity}/{empresa}_{year}_{month}_{day}.csv'
        client.put_object(os.environ.get("LANDING"), name, data=csv_buffer, length=len(csv_data), content_type='application/csv')
        print(name)
        print(empresa)
        
        return len(df.index)
    
    except Exception as e:
        print(e)
        raise Exception('xxxxxx')


if __name__ == '__main__':
    try:
        df = query_result()
        total_linhas_frame = send_data_to_minio(init_minio_connect(),df)
    except Exception as e:
        print(e)


