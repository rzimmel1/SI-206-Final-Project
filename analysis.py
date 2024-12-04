import sqlite3
import matplotlib.pyplot as plt
import pandas as pd
import re

# Function to normalize city names
def normalize_city_names(df, column):
    df[column] = df[column].str.lower()
    return df

# Function to fetch average weather data
def fetch_weather_averages(db_path):
    conn = sqlite3.connect(db_path)
    query = '''
    SELECT c.city_name, 
           avg.average_temperature_2m as avg_temp,
           avg.average_relative_humidity_2m as avg_humidity,
           avg.average_windspeed_10m as avg_windspeed,
           avg.average_precipitation as avg_precip
    FROM city_averages avg
    JOIN cities c ON avg.city_id = c.city_id
    '''
    weather_df = pd.read_sql_query(query, conn)
    conn.close()
    weather_df = normalize_city_names(weather_df, 'city_name')
    print("Weather Data:")
    print(weather_df)
    return weather_df

# Function to fetch car prices by year and city
def fetch_car_prices(db_path):
    conn = sqlite3.connect(db_path)
    query = '''
    SELECT c.id as city_id, c.city as city_name, car.year, p.price
    FROM prices p
    JOIN cars car ON p.car_id = car.id
    JOIN cities c ON p.city_id = c.id
    '''
    price_data = pd.read_sql_query(query, conn)
    conn.close()
    price_data = normalize_city_names(price_data, 'city_name')
    print("Price Data:")
    print(price_data)
    return price_data

# Process prices to calculate average depreciation
def calculate_depreciation(price_data):
    depreciation_data = []

    for city_id in price_data['city_id'].unique():
        city_prices = price_data[price_data['city_id'] == city_id]
        avg_price_new, avg_price_old = None, None

        for year in [2018, 2024]:
            avg_price = city_prices[city_prices['year'] == year]['price'].astype(str).str.replace(',', '').str.replace(r'[^\d.]', '', regex=True).astype(float).mean()
            if year == 2024:
                avg_price_new = avg_price
            elif year == 2018:
                avg_price_old = avg_price

        if avg_price_new and avg_price_old:
            depreciation = ((avg_price_new - avg_price_old) / avg_price_new) * 100
            city_name = city_prices['city_name'].iloc[0]
            depreciation_data.append({'city': city_name, 'city_id': city_id, 'depreciation': depreciation, 'avg_new_price': avg_price_new, 'avg_old_price': avg_price_old})

    depreciation_df = pd.DataFrame(depreciation_data)
    print("Depreciation Data:")
    print(depreciation_df)
    return depreciation_df

# Function to merge dataframes
def merge_data(weather_df, depreciation_df):
    merged_df = pd.merge(depreciation_df, weather_df, left_on='city', right_on='city_name')
    print("Merged Data:")
    print(merged_df)
    return merged_df

# Function to plot the data
def plot_data(merged_df):
    plt.figure(figsize=(12, 6))

    # Scatter plot for temperature vs depreciation
    plt.subplot(1, 2, 1)
    plt.scatter(merged_df['avg_temp'], merged_df['depreciation'], color='blue', label='Cities')
    plt.title('Temperature vs Car Depreciation')
    plt.xlabel('Average Temperature (°C)')
    plt.ylabel('Depreciation (%)')
    plt.ylim(40, 50)  # Set y-axis limits to focus on the data range
    plt.axhline(y=0, color='gray', linestyle='--', linewidth=0.7)
    for i, city in enumerate(merged_df['city']):
        plt.annotate(city, (merged_df['avg_temp'][i], merged_df['depreciation'][i]), textcoords="offset points", xytext=(0,5), ha='center')

    # Scatter plot for humidity vs depreciation
    plt.subplot(1, 2, 2)
    plt.scatter(merged_df['avg_humidity'], merged_df['depreciation'], color='green', label='Cities')
    plt.title('Humidity vs Car Depreciation')
    plt.xlabel('Average Humidity (%)')
    plt.ylabel('Depreciation (%)')
    plt.ylim(40, 50)  # Set y-axis limits to focus on the data range
    plt.axhline(y=0, color='gray', linestyle='--', linewidth=0.7)
    for i, city in enumerate(merged_df['city']):
        plt.annotate(city, (merged_df['avg_humidity'][i], merged_df['depreciation'][i]), textcoords="offset points", xytext=(0,5), ha='center')

    plt.tight_layout()
    plt.show()

def main():
    weather_db_path = 'weather_data.db'
    car_db_path = 'car_data.db'

    # Fetch data
    weather_df = fetch_weather_averages(weather_db_path)
    price_data = fetch_car_prices(car_db_path)

    # Calculate depreciation
    depreciation_df = calculate_depreciation(price_data)

    # Merge data
    merged_df = merge_data(weather_df, depreciation_df)

    # Plot data
    plot_data(merged_df)

if __name__ == "__main__":
    main()