import sqlite3
import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

# List of cities with their coordinates (latitude, longitude)
cities = [
    {"name": "Berlin", "latitude": 52.52, "longitude": 13.41},
    {"name": "Paris", "latitude": 48.8566, "longitude": 2.3522},
    {"name": "New York", "latitude": 40.7128, "longitude": -74.0060},
    {"name": "Tokyo", "latitude": 35.6895, "longitude": 139.6917}
]

# Function to create the database and tables, dropping the old tables if they exist
def initialize_db():
    conn = sqlite3.connect('weather_data.db')
    c = conn.cursor()

    # Drop the tables if they exist
    c.execute('DROP TABLE IF EXISTS hourly_climate')
    c.execute('DROP TABLE IF EXISTS cities')

    # Create the cities table
    c.execute('''
        CREATE TABLE cities (
            city_id INTEGER PRIMARY KEY AUTOINCREMENT,
            city_name TEXT UNIQUE,
            latitude REAL,
            longitude REAL
        )
    ''')

    # Create the hourly_climate table
    c.execute('''
        CREATE TABLE hourly_climate (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            city_id INTEGER,
            date TEXT,
            temperature_2m REAL,
            relative_humidity_2m REAL,
            windspeed_10m REAL,
            precipitation REAL,
            FOREIGN KEY (city_id) REFERENCES cities(city_id),
            UNIQUE(city_id, date)
        )
    ''')
    conn.commit()
    conn.close()

# Function to insert cities into the database
def insert_cities_into_db():
    conn = sqlite3.connect('weather_data.db')
    c = conn.cursor()

    for city in cities:
        try:
            c.execute('''
                INSERT INTO cities (city_name, latitude, longitude) VALUES (?, ?, ?)
            ''', (city['name'], city['latitude'], city['longitude']))
        except sqlite3.IntegrityError:
            # Ignore duplicate data based on the unique constraint on city_name
            continue

    conn.commit()
    conn.close()

# Function to fetch city_id by city_name
def get_city_id(city_name):
    conn = sqlite3.connect('weather_data.db')
    c = conn.cursor()
    c.execute('SELECT city_id FROM cities WHERE city_name=?', (city_name,))
    city_id = c.fetchone()[0]
    conn.close()
    return city_id

# Function to insert climate data into the database
def insert_data_to_db(city_id, data):
    conn = sqlite3.connect('weather_data.db')
    c = conn.cursor()

    for row in data.itertuples(index=False):
        try:
            c.execute('''
                INSERT INTO hourly_climate (city_id, date, temperature_2m, relative_humidity_2m, windspeed_10m, precipitation)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (city_id, row.date, row.temperature_2m, row.relative_humidity_2m, row.windspeed_10m, row.precipitation))
        except sqlite3.IntegrityError:
            # Ignore duplicate data based on the unique constraint on city_id and date
            continue

    conn.commit()
    conn.close()

# Function to fetch data from the API
def fetch_weather_data(latitude, longitude):
    url = "https://archive-api.open-meteo.com/v1/archive"  # Ensure we're hitting the right historical endpoint
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": "2024-11-16",
        "end_date": "2024-11-29",
        "hourly": "temperature_2m,relative_humidity_2m,windspeed_10m,precipitation"
    }
    
    responses = openmeteo.weather_api(url, params=params)
    response = responses[0]

    # Process hourly data
    hourly = response.Hourly()
    hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
    hourly_humidity = hourly.Variables(1).ValuesAsNumpy()
    hourly_wind_speed = hourly.Variables(2).ValuesAsNumpy()
    hourly_precipitation = hourly.Variables(3).ValuesAsNumpy()

    hourly_data = {
        "date": pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left"
        )
    }
    hourly_data["temperature_2m"] = hourly_temperature_2m
    hourly_data["relative_humidity_2m"] = hourly_humidity
    hourly_data["windspeed_10m"] = hourly_wind_speed
    hourly_data["precipitation"] = hourly_precipitation

    return pd.DataFrame(data=hourly_data)

# Main function to orchestrate data fetching and storing process
def main():
    initialize_db()
    insert_cities_into_db()
    
    max_rows_to_fetch = 25
    
    for city in cities:
        city_name = city["name"]
        latitude = city["latitude"]
        longitude = city["longitude"]
        
        city_id = get_city_id(city_name)
        
        # Fetch existing row count for the specific city from database
        conn = sqlite3.connect('weather_data.db')
        c = conn.cursor()
        c.execute('SELECT COUNT(*) FROM hourly_climate WHERE city_id=?', (city_id,))
        existing_row_count = c.fetchone()[0]
        conn.close()

        # Fetch and insert data in chunks
        if existing_row_count < 100:
            df = fetch_weather_data(latitude, longitude)

            # Ensure date column is a string
            df['date'] = df['date'].astype(str)

            data_chunk = df.iloc[existing_row_count:existing_row_count + max_rows_to_fetch]
            
            insert_data_to_db(city_id, data_chunk)
            
            existing_row_count += len(data_chunk)
            print(f"Inserted {len(data_chunk)} rows for {city_name}")

if __name__ == "__main__":
    main()
