import requests
import json
import sqlite3
from datetime import datetime, timedelta

API_KEY = 'YOUR_API_KEY'
BASE_URL = 'http://api.weatherstack.com/historical'
CITIES = ['Delhi', 'Mumbai', 'Bangalore', 'Kolkata', 'Chennai', 'Hyderabad', 'Pune']
START_DATE = '2010-01-01'
END_DATE = datetime.now().strftime('%Y-%m-%d')

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
    conn = sqlite3.connect('weather_data.db')
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

def fetch_and_store_all():
    current_date = datetime.strptime(START_DATE, '%Y-%m-%d')
    end_date = datetime.strptime(END_DATE, '%Y-%m-%d')
    delta = timedelta(days=1)

    while current_date <= end_date:
        for city in CITIES:
            date_str = current_date.strftime('%Y-%m-%d')
            data = fetch_weather_data(city, date_str)
            store_data_to_db(data, city)
        current_date += delta

fetch_and_store_all()
