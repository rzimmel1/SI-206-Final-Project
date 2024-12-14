import sqlite3
import matplotlib.pyplot as plt
import pandas as pd


def normalize_city_names(df, column):
    df[column] = df[column].str.lower()
    return df

# Map "aurora" to "denver" in city names
def map_city_names(df, column):
    df[column] = df[column].replace("aurora", "denver")
    return df

# Fetch combined depreciation and weather data from the unified database
def fetch_combined_data(db_path):
    conn = sqlite3.connect(db_path)
    query = '''
    SELECT c.city, 
           c.state, 
           avg.average_temperature_2m as avg_temp,
           avg.average_relative_humidity_2m as avg_humidity,
           avg.average_windspeed_10m as avg_windspeed,
           avg.average_precipitation as avg_precip,
           dep.depreciation,
           dep.avg_new_price,
           dep.avg_old_price
    FROM car_depreciation dep
    JOIN city_averages avg ON dep.city_id = avg.city_id
    JOIN cities c ON dep.city_id = c.id
    '''
    combined_df = pd.read_sql_query(query, conn)
    conn.close()
    combined_df = normalize_city_names(combined_df, 'city')
    combined_df = map_city_names(combined_df, 'city')
    print("Combined Data:")
    print(combined_df)
    return combined_df

# Plot the data
def plot_data(merged_df):
    plt.figure(figsize=(18, 6))

    colors = ['blue', 'green', 'red', 'purple', 'orange']
    city_colors = dict(zip(merged_df['city'].unique(), colors))

    # Scatter plot for temperature vs depreciation
    plt.subplot(1, 3, 1)
    for city, color in city_colors.items():
        city_data = merged_df[merged_df['city'] == city]
        plt.scatter(city_data['avg_temp'], city_data['depreciation'], color=color, label=city.title())
        for i in range(len(city_data)):
            plt.annotate(city_data['city'].values[i].title(), 
                         (city_data['avg_temp'].values[i], city_data['depreciation'].values[i]), 
                         textcoords="offset points", xytext=(0,10), ha='center')

    plt.title('Temperature vs Car Depreciation')
    plt.xlabel('Average Daily Temperature (°C)')
    plt.ylabel('Average Depreciation (%)')
    plt.ylim(35, 52)
    plt.axhline(y=40, color='gray', linestyle='--', linewidth=0.7)
    plt.legend()

    # Scatter plot for humidity vs depreciation
    plt.subplot(1, 3, 2)
    for city, color in city_colors.items():
        city_data = merged_df[merged_df['city'] == city]
        plt.scatter(city_data['avg_humidity'], city_data['depreciation'], color=color, label=city.title())
        for i in range(len(city_data)):
            plt.annotate(city_data['city'].values[i].title(), 
                         (city_data['avg_humidity'].values[i], city_data['depreciation'].values[i]), 
                         textcoords="offset points", xytext=(0,10), ha='center')
    
    plt.title('Humidity vs Car Depreciation')
    plt.xlabel('Average Daily Humidity (%)')
    plt.ylabel('Average Depreciation (%)')
    plt.ylim(35, 52)
    plt.axhline(y=40, color='gray', linestyle='--', linewidth=0.7)
    plt.legend()

    # Scatter plot for precipitation vs depreciation
    plt.subplot(1, 3, 3)
    for city, color in city_colors.items():
        city_data = merged_df[merged_df['city'] == city]
        plt.scatter(city_data['avg_precip'], city_data['depreciation'], color=color, label=city.title())
        for i in range(len(city_data)):
            plt.annotate(city_data['city'].values[i].title(), 
                         (city_data['avg_precip'].values[i], city_data['depreciation'].values[i]), 
                         textcoords="offset points", xytext=(0,10), ha='center')
    
    plt.title('Precipitation vs Car Depreciation')
    plt.xlabel('Average Daily Precipitation (mm)')
    plt.ylabel('Average Depreciation (%)')
    plt.ylim(35, 52)
    plt.axhline(y=40, color='gray', linestyle='--', linewidth=0.7)
    plt.legend()

    plt.tight_layout()
    plt.show()


def main():
    db_path = 'unified_data.db'

    combined_df = fetch_combined_data(db_path)

    plot_data(combined_df)

if __name__ == "__main__":
    main()