import sqlite3
import matplotlib.pyplot as plt
import pandas as pd

# Function to fetch weather averages
def fetch_weather_averages(db_path):
    conn = sqlite3.connect(db_path)
    query = '''
    SELECT c.city_name, 
           AVG(avg.temperature_2m) as avg_temp,
           AVG(avg.relative_humidity_2m) as avg_humidity,
           AVG(avg.windspeed_10m) as avg_windspeed,
           AVG(avg.precipitation) as avg_precip
    FROM city_averages avg
    JOIN cities c ON avg.city_id = c.city_id
    GROUP BY c.city_name
    '''
    weather_df = pd.read_sql_query(query, conn)
    conn.close()
    return weather_df

# Function to fetch car depreciation data
def fetch_depreciation_data(db_path):
    conn = sqlite3.connect(db_path)
    query = '''
    SELECT c.city, c.state, 
           d.depreciation, 
           d.avg_new_price, 
           d.avg_old_price
    FROM depreciation_by_city d
    JOIN cities c ON d.city_id = c.id
    '''
    dep_df = pd.read_sql_query(query, conn)
    conn.close()
    return dep_df

# Function to merge dataframes
def merge_data(weather_df, dep_df):
    merged_df = pd.merge(dep_df, weather_df, left_on='city', right_on='city_name')
    return merged_df

# Function to plot the data
def plot_data(merged_df):
    plt.figure(figsize=(12, 8))

    # Scatter plot for temperature vs depreciation
    plt.subplot(2, 2, 1)
    plt.scatter(merged_df['avg_temp'], merged_df['depreciation'])
    plt.title('Temperature vs Car Depreciation')
    plt.xlabel('Average Temperature (°C)')
    plt.ylabel('Depreciation (%)')

    # Scatter plot for humidity vs depreciation
    plt.subplot(2, 2, 2)
    plt.scatter(merged_df['avg_humidity'], merged_df['depreciation'])
    plt.title('Humidity vs Car Depreciation')
    plt.xlabel('Average Humidity (%)')
    plt.ylabel('Depreciation (%)')

    # Scatter plot for windspeed vs depreciation
    plt.subplot(2, 2, 3)
    plt.scatter(merged_df['avg_windspeed'], merged_df['depreciation'])
    plt.title('Wind Speed vs Car Depreciation')
    plt.xlabel('Average Wind Speed (m/s)')
    plt.ylabel('Depreciation (%)')

    # Scatter plot for precipitation vs depreciation
    plt.subplot(2, 2, 4)
    plt.scatter(merged_df['avg_precip'], merged_df['depreciation'])
    plt.title('Precipitation vs Car Depreciation')
    plt.xlabel('Average Precipitation (mm)')
    plt.ylabel('Depreciation (%)')

    plt.tight_layout()
    plt.show()

def main():
    weather_db_path = 'weather_data.db'
    car_db_path = 'car_data.db'

    # Fetch data
    weather_df = fetch_weather_averages(weather_db_path)
    depreciation_df = fetch_depreciation_data(car_db_path)

    # Merge data
    merged_df = merge_data(weather_df, depreciation_df)

    # Plot data
    plot_data(merged_df)

if __name__ == "__main__":
    main()
