import sqlite3
import matplotlib.pyplot as plt
import pandas as pd
import re

#city names
def normalize_city_names(df, column):
    df[column] = df[column].str.lower()
    return df

#Aurora to Denver
def map_city_names(df, column):
    df[column] = df[column].replace("aurora", "denver")
    return df

#average weather data
def fetch_weather_averages(db_path):
    conn = sqlite3.connect(db_path)
    query = '''
    SELECT c.city_name, 
           avg.average_temperature_2m as avg_temp,
           avg.average_relative_humidity_2m as avg_humidity,
           avg.average_windspeed_10m as avg_windspeed
    FROM city_averages avg
    JOIN cities c ON avg.city_id = c.city_id
    GROUP BY c.city_name
    '''
    weather_df = pd.read_sql_query(query, conn)
    conn.close()
    weather_df = normalize_city_names(weather_df, 'city_name')
    weather_df = map_city_names(weather_df, 'city_name')
    print("Weather Data:")
    print(weather_df)
    return weather_df

#car prices
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
    price_data = map_city_names(price_data, 'city_name')
    print("Price Data:")
    print(price_data)
    return price_data

#calculate average depreciation
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

#merge dataframes
def merge_data(weather_df, depreciation_df):
    merged_df = pd.merge(depreciation_df, weather_df, left_on='city', right_on='city_name')
    print("Merged Data:")
    print(merged_df)
    return merged_df

#plot the data
def plot_data(merged_df):
    plt.figure(figsize=(12, 6))

    colors = ['blue', 'green', 'red', 'purple', 'orange']
    city_colors = dict(zip(merged_df['city'].unique(), colors))

    # Scatter plot for temperature vs depreciation
    plt.subplot(1, 2, 1)
    for city, color in city_colors.items():
        city_data = merged_df[merged_df['city'] == city]
        plt.scatter(city_data['avg_temp'], city_data['depreciation'], color=color, label=city.title())
        for i in range(len(city_data)):
            plt.annotate(city_data['city'].values[i].title(), 
                         (city_data['avg_temp'].values[i], city_data['depreciation'].values[i]), 
                         textcoords="offset points", xytext=(0,10), ha='center')

    plt.title('Temperature vs Car Depreciation')
    plt.xlabel('Average Temperature (°C)')
    plt.ylabel('Depreciation (%)')
    plt.ylim(35, 50)
    plt.axhline(y=40, color='gray', linestyle='--', linewidth=0.7)
    plt.legend()

    # Scatter plot for humidity vs depreciation
    plt.subplot(1, 2, 2)
    for city, color in city_colors.items():
        city_data = merged_df[merged_df['city'] == city]
        plt.scatter(city_data['avg_humidity'], city_data['depreciation'], color=color, label=city.title())
        for i in range(len(city_data)):
            plt.annotate(city_data['city'].values[i].title(), 
                         (city_data['avg_humidity'].values[i], city_data['depreciation'].values[i]), 
                         textcoords="offset points", xytext=(0,10), ha='center')
    
    plt.title('Humidity vs Car Depreciation')
    plt.xlabel('Average Humidity (%)')
    plt.ylabel('Depreciation (%)')
    plt.ylim(35, 50)
    plt.axhline(y=40, color='gray', linestyle='--', linewidth=0.7)
    plt.legend()

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