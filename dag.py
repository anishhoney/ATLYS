from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.sqlite_operator import SqliteOperator
from datetime import datetime, timedelta
import requests
import json
import sqlite3

API_KEY = 'YOUR_ACTUAL_API_KEY'
BASE_URL = 'http://api.weatherstack.com/historical'
CITIES = ['Delhi', 'Mumbai', 'Bangalore', 'Kolkata', 'Chennai', 'Hyderabad', 'Pune']
YESTERDAY = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

def fetch_weather_data(city, date):
    params = {
        'access_key': API_KEY,
        'query': city,
        'historical_date': date,
        'hourly': 1
    }
    response = requests.get(BASE_URL, params=params)
    data = response.json()
    return data

def store_data_to_db(data, city):
    conn = sqlite3.connect('/path/to/your/weather_data.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS weather_data (
        date TEXT, 
        city TEXT,
        sunrise TEXT, 
        sunset TEXT, 
        moonrise TEXT, 
        moonset TEXT, 
        moon_phase TEXT, 
        moon_illumination TEXT, 
        mintemp REAL, 
        maxtemp REAL, 
        avgtemp REAL, 
        totalsnow REAL, 
        sunhour REAL, 
        uv_index REAL,
        hourly TEXT)''')

    for date, daily_data in data['historical'].items():
        c.execute('''INSERT INTO weather_data (date, city, sunrise, sunset, moonrise, moonset, moon_phase, moon_illumination, 
                    mintemp, maxtemp, avgtemp, totalsnow, sunhour, uv_index, hourly) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', 
                  (date, city, daily_data['astro']['sunrise'], daily_data['astro']['sunset'], daily_data['astro']['moonrise'], 
                   daily_data['astro']['moonset'], daily_data['astro']['moon_phase'], daily_data['astro']['moon_illumination'], 
                   daily_data['mintemp'], daily_data['maxtemp'], daily_data['avgtemp'], daily_data['totalsnow'], 
                   daily_data['sunhour'], daily_data['uv_index'], json.dumps(daily_data['hourly'])))

    conn.commit()
    conn.close()

def fetch_and_store_yesterday():
    for city in CITIES:
        data = fetch_weather_data(city, YESTERDAY)
        store_data_to_db(data, city)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'daily_weather_data',
    default_args=default_args,
    description='A simple daily weather data DAG',
    schedule_interval=timedelta(days=1),
)

task1 = PythonOperator(
    task_id='fetch_and_store_yesterday',
    python_callable=fetch_and_store_yesterday,
    dag=dag,
)

task2 = SqliteOperator(
    task_id='create_indexes',
    sqlite_conn_id='your_sqlite_conn_id',
    sql='''
    CREATE INDEX IF NOT EXISTS idx_city_date ON weather_data(city, date);
    CREATE INDEX IF NOT EXISTS idx_date_city ON weather_data(date, city);
    CREATE INDEX IF NOT EXISTS idx_city ON weather_data(city);
    CREATE INDEX IF NOT EXISTS idx_date ON weather_data(date);
    ''',
    dag=dag,
)

task3 = SqliteOperator(
    task_id='daily_min_temp',
    sqlite_conn_id='your_sqlite_conn_id',
    sql='''
    SELECT city, date, mintemp 
    FROM weather_data 
    WHERE date = CURRENT_DATE - INTERVAL '1' DAY;
    ''',
    dag=dag,
)

task4 = SqliteOperator(
    task_id='daily_max_temp',
    sqlite_conn_id='your_sqlite_conn_id',
    sql='''
    SELECT city, date, maxtemp 
    FROM weather_data 
    WHERE date = CURRENT_DATE - INTERVAL '1' DAY;
    ''',
    dag=dag,
)

task5 = SqliteOperator(
    task_id='daily_sunset',
    sqlite_conn_id='your_sqlite_conn_id',
    sql='''
    SELECT city, date, sunset 
    FROM weather_data 
    WHERE date = CURRENT_DATE - INTERVAL '1' DAY;
    ''',
    dag=dag,
)

task6 = SqliteOperator(
    task_id='daily_sunrise',
    sqlite_conn_id='your_sqlite_conn_id',
    sql='''
    SELECT city, date, sunrise 
    FROM weather_data 
    WHERE date = CURRENT_DATE - INTERVAL '1' DAY;
    ''',
    dag=dag,
)

task7 = SqliteOperator(
    task_id='hourly_temp',
    sqlite_conn_id='your_sqlite_conn_id',
    sql='''
    SELECT city, date, hourly 
    FROM weather_data 
    WHERE date = CURRENT_DATE - INTERVAL '1' DAY;
    ''',
    dag=dag,
)

# Define task dependencies
task1 >> task2
task2 >> [task3, task4, task5, task6, task7]
