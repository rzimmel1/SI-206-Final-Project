import requests
import sqlite3
import json
import os

# Step 1: Define your list of cities
cities = ["London,UK", "New York,US", "Tokyo,JP", "Paris,FR"]
api_key = "EWUAQ9QALCC8B7WEYM5NQ2CGZ"
start_date = "2023-01-01"
end_date = "2023-01-31"
max_records_per_run = 25

# Step 2: Set up SQLite database connection and cursor
db_path = 'weather_data.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Step 3: Create a table to store the data (if it doesn't already exist)
cursor.execute('''
CREATE TABLE IF NOT EXISTS weather (
    city TEXT,
    datetime TEXT,
    temp REAL,
    feelslike REAL,
    humidity REAL,
    dew REAL,
    precip REAL,
    windspeed REAL,
    winddir REAL,
    visibility REAL,
    conditions TEXT,
    cloudcover REAL,
    solarradiation REAL,
    tempmax REAL,
    tempmin REAL,
    sunrise TEXT,
    sunset TEXT,
    forecast_basis_date TEXT,
    PRIMARY KEY (city, datetime)
)
''')

def fetch_weather_data(city, start_date, end_date, api_key):
    url = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{city}/{start_date}/{end_date}?key={api_key}&include=days&forecastBasisDate={start_date}"
    response = requests.get(url)
    return response.json()

def get_last_processed_datetime(city):
    cursor.execute('SELECT MAX(datetime) FROM weather WHERE city=?', (city,))
    result = cursor.fetchone()[0]
    return result

def store_weather_data(records):
    cursor.executemany('''
    INSERT OR IGNORE INTO weather (city, datetime, temp, feelslike, humidity, dew, precip, windspeed, winddir, visibility, conditions, cloudcover, solarradiation, tempmax, tempmin, sunrise, sunset, forecast_basis_date)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', records)
    conn.commit()

for city in cities:
    last_processed_datetime = get_last_processed_datetime(city)
    if last_processed_datetime:
        start_date = last_processed_datetime.split("T")[0]
    
    data = fetch_weather_data(city, start_date, end_date)
    weather_records = []
    count = 0
    
    for day in data['days']:
        record = (
            city,                        # city
            day['datetime'],             # datetime
            day.get('temp', None),       # temp
            day.get('feelslike', None),  # feelslike
            day.get('humidity', None),   # humidity
            day.get('dew', None),        # dew
            day.get('precip', None),     # precip
            day.get('windspeed', None),  # windspeed
            day.get('winddir', None),    # winddir
            day.get('visibility', None), # visibility
            day.get('conditions', ""),   # conditions
            day.get('cloudcover', None), # cloudcover
            day.get('solarradiation', None), # solarradiation
            day.get('tempmax', None),    # tempmax
            day.get('tempmin', None),    # tempmin
            day.get('sunrise', ""),      # sunrise
            day.get('sunset', ""),       # sunset
            start_date                   # forecast_basis_date
        )
        weather_records.append(record)
        count += 1
        if count >= max_records_per_run:
            break
    
    store_weather_data(weather_records)

conn.close()
print("Data inserted successfully")
