import sqlite3
import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry

# Setup API 
cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

# Cities with their coordinates 
cities = [
    {"name": "Phoenix", "latitude": 33.4484, "longitude": -112.0740},
    {"name": "Denver", "latitude": 39.7392, "longitude": -104.9903},
    {"name": "Seattle", "latitude": 47.6062, "longitude": -122.3321},
    {"name": "Minneapolis", "latitude": 44.9778, "longitude": -93.2650},
    {"name": "Miami", "latitude": 25.7617, "longitude": -80.1918}
]

# Create the database and tables
def initialize_db():
    conn = sqlite3.connect('weather_data.db')
    c = conn.cursor()

    # Create the cities table if it does not exist
    c.execute('''
        CREATE TABLE IF NOT EXISTS cities (
            city_id INTEGER PRIMARY KEY AUTOINCREMENT,
            city_name TEXT UNIQUE,
            latitude REAL,
            longitude REAL
        )
    ''')

    # Create the hourly_climate table if it does not exist
    c.execute('''
        CREATE TABLE IF NOT EXISTS hourly_climate (
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

    # Create the city_averages table if it does not exist
    c.execute('''
        CREATE TABLE IF NOT EXISTS city_averages (
            city_id INTEGER PRIMARY KEY,
            average_temperature_2m REAL,
            average_relative_humidity_2m REAL,
            average_windspeed_10m REAL,
            average_precipitation REAL,
            FOREIGN KEY (city_id) REFERENCES cities(city_id)
        )
    ''')

    conn.commit()
    conn.close()

# Insert cities
def insert_cities_into_db():
    conn = sqlite3.connect('weather_data.db')
    c = conn.cursor()

    for city in cities:
        try:
            c.execute('''
                INSERT INTO cities (city_name, latitude, longitude) VALUES (?, ?, ?)
            ''', (city['name'], city['latitude'], city['longitude']))
        except sqlite3.IntegrityError:
            continue

    conn.commit()
    conn.close()

# Fetch city_id by city_name
def get_city_id(city_name):
    conn = sqlite3.connect('weather_data.db')
    c = conn.cursor()
    c.execute('SELECT city_id FROM cities WHERE city_name=?', (city_name,))
    city_id = c.fetchone()[0]
    conn.close()
    return city_id

# Insert climate data into the database
def insert_data_to_db(city_id, data, rows_needed):
    conn = sqlite3.connect('weather_data.db')
    c = conn.cursor()
    rows_inserted = 0

    for row in data.itertuples(index=False):
        if rows_inserted >= rows_needed:
            break
        try:
            c.execute('''
                INSERT INTO hourly_climate (city_id, date, temperature_2m, relative_humidity_2m, windspeed_10m, precipitation)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (city_id, row.date, row.temperature_2m, row.relative_humidity_2m, row.windspeed_10m, row.precipitation))
            rows_inserted += 1
        except sqlite3.IntegrityError:
            continue

    conn.commit()
    conn.close()
    return rows_inserted


# Fetch data from the API
def fetch_weather_data(latitude, longitude, start_offset):
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": "2018-01-01",
        "end_date": "2024-12-03",
        "hourly": "temperature_2m,relative_humidity_2m,windspeed_10m,precipitation"
    }
    
    responses = openmeteo.weather_api(url, params=params)
    response = responses[0]
    hourly = response.Hourly()
    hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
    hourly_humidity = hourly.Variables(1).ValuesAsNumpy()
    hourly_wind_speed = hourly.Variables(2).ValuesAsNumpy()
    hourly_precipitation = hourly.Variables(3).ValuesAsNumpy()

    min_len = min(len(hourly_temperature_2m), len(hourly_humidity), len(hourly_wind_speed), len(hourly_precipitation))
    time_range = pd.date_range(
        start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
        periods=min_len,
        freq=pd.Timedelta(seconds=hourly.Interval())
    )

    # Convert to DataFrame for easier manipulation
    df = pd.DataFrame({
        'time': time_range,
        'temperature_2m': hourly_temperature_2m,
        'relative_humidity_2m': hourly_humidity,
        'windspeed_10m': hourly_wind_speed,
        'precipitation': hourly_precipitation
    })

    # Filter to get one hour per day (e.g., 12:00 PM)
    df['date'] = df['time'].dt.date
    df['hour'] = df['time'].dt.hour
    df_filtered = df[df['hour'] == 12]  # Change 12 to any specific hour you want

    # Reset index and return the filtered data
    df_filtered = df_filtered.reset_index(drop=True)
    return df_filtered

# Calculate and store average weather data
def store_average_weather():
    conn = sqlite3.connect('weather_data.db')
    c = conn.cursor()
    
    for city in cities:
        city_name = city["name"]
        city_id = get_city_id(city_name)
        
        c.execute('''
            SELECT 
                AVG(temperature_2m) AS avg_temp, 
                AVG(relative_humidity_2m) AS avg_humidity, 
                AVG(windspeed_10m) AS avg_windspeed,
                AVG(precipitation) AS avg_precip
            FROM hourly_climate 
            WHERE city_id=?
        ''', (city_id,))
        
        averages = c.fetchone()
        
        c.execute('''
            INSERT OR REPLACE INTO city_averages (city_id, average_temperature_2m, average_relative_humidity_2m, average_windspeed_10m, average_precipitation)
            VALUES (?, ?, ?, ?, ?)
        ''', (city_id, averages[0], averages[1], averages[2], averages[3]))

    conn.commit()
    conn.close()

# Main function 
def main():
    initialize_db()
    insert_cities_into_db()
    
    total_rows_inserted = 0
    max_total_rows_per_run = 25
    rows_per_city = max_total_rows_per_run // len(cities)

    for city in cities:
        city_name = city["name"]
        latitude = city["latitude"]
        longitude = city["longitude"]

        city_id = get_city_id(city_name)
        
        conn = sqlite3.connect('weather_data.db')
        c = conn.cursor()
        c.execute('SELECT COUNT(*) FROM hourly_climate WHERE city_id=?', (city_id,))
        existing_row_count = c.fetchone()[0]
        conn.close()

        if existing_row_count < 20:
            start_offset = existing_row_count
            df = fetch_weather_data(latitude, longitude, start_offset)

            df['date'] = df['date'].astype(str)
            rows_needed = min(rows_per_city, 100 - existing_row_count)
            rows_inserted = insert_data_to_db(city_id, df, rows_needed)
            total_rows_inserted += rows_inserted
        else:
            start_offset = existing_row_count
            df = fetch_weather_data(latitude, longitude, start_offset)
            df['date'] = df['date'].astype(str)
            rows_inserted = insert_data_to_db(city_id, df, len(df))
            total_rows_inserted += rows_inserted
            store_average_weather()

if __name__ == "__main__":
    main()