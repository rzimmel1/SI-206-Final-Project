# calculates the depreciation of the cars in our database and calculates weather averages

import sqlite3
import re

def get_average_price_by_year_and_city(year, city_id):
    conn = sqlite3.connect('unified_data.db')
    c = conn.cursor()
    
    c.execute('''SELECT price FROM prices 
                 JOIN cars ON prices.car_id = cars.id 
                 WHERE cars.year = ? AND prices.city_id = ?''', (year, city_id))
    prices = [float(re.sub(r'[^\d.]', '', price[0])) for price in c.fetchall()]
    
    conn.close()
    
    if prices:
        return sum(prices) / len(prices)
    else:
        return None

def calculate_average_depreciation_by_city():
    conn = sqlite3.connect('unified_data.db')
    c = conn.cursor()
    
    c.execute('''SELECT id, city, state FROM cities''')
    cities = c.fetchall()
    
    depreciation_by_city = {}
    
    for city_id, city, state in cities:
        avg_price_new = get_average_price_by_year_and_city(2024, city_id)
        avg_price_6_year_old = get_average_price_by_year_and_city(2018, city_id)
        
        if avg_price_new and avg_price_6_year_old:
            depreciation = ((avg_price_new - avg_price_6_year_old) / avg_price_new) * 100
            depreciation_by_city[f"{city}, {state}"] = (depreciation, avg_price_new, avg_price_6_year_old)
        elif city == 'tesla':
            avg_price_5_year_old = get_average_price_by_year_and_city(2023, city_id)
            if avg_price_new and avg_price_5_year_old:
                depreciation = ((avg_price_new - avg_price_5_year_old) / avg_price_new) * 100
                depreciation_by_city[f"{city}, {state}"] = (depreciation, avg_price_new, avg_price_5_year_old)
    
    conn.close()
    
    return depreciation_by_city

def store_depreciation_data(depreciation_by_city):
    conn = sqlite3.connect('unified_data.db')
    c = conn.cursor()
    
    for city_state, data in depreciation_by_city.items():
        city, state = city_state.split(", ")
        depreciation, avg_new_price, avg_old_price = data
        c.execute('''
            INSERT OR REPLACE INTO car_depreciation 
            (city_id, city, state, depreciation, avg_new_price, avg_old_price)
            VALUES (
                (SELECT id FROM cities WHERE city = ? AND state = ?), 
                ?, ?, ?, ?, ?
            )
        ''', (city, state, city, state, depreciation, avg_new_price, avg_old_price))
    
    conn.commit()
    conn.close()

def store_average_weather():
    conn = sqlite3.connect('unified_data.db') 
    c = conn.cursor()

    c.execute('SELECT id, city FROM cities')
    cities = c.fetchall()

    for city_id, city_name in cities:
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
        if averages is None:
            continue
        
        c.execute('''
            INSERT OR REPLACE INTO city_averages (city_id, average_temperature_2m, average_relative_humidity_2m, average_windspeed_10m, average_precipitation)
            VALUES (?, ?, ?, ?, ?)
        ''', (city_id, averages[0], averages[1], averages[2], averages[3]))

    conn.commit()
    conn.close()

def fetch_weather_data_by_city():
    conn = sqlite3.connect('unified_data.db')
    c = conn.cursor()
    
    c.execute('''
    SELECT c.city, c.state, ca.average_temperature_2m, ca.average_relative_humidity_2m, ca.average_windspeed_10m, ca.average_precipitation
    FROM city_averages ca
    JOIN cities c ON ca.city_id = c.id
    ''')
    
    weather_data = c.fetchall()
    conn.close()
    
    weather_by_city = {}
    for city, state, avg_temp, avg_humidity, avg_windspeed, avg_precip in weather_data:
        weather_by_city[f"{city}, {state}"] = (avg_temp, avg_humidity, avg_windspeed, avg_precip)
    
    return weather_by_city

def main():
    store_average_weather()
    
    # Calculate and store average depreciation by city using all cars
    depreciation_by_city = calculate_average_depreciation_by_city()
    store_depreciation_data(depreciation_by_city)
    
    # Fetch weather data by city
    weather_by_city = fetch_weather_data_by_city()
    
    with open('depreciation_report.txt', 'w') as file:
        for city, data in depreciation_by_city.items():
            depreciation, avg_price_new, avg_price_old = data
            avg_temp, avg_humidity, avg_windspeed, avg_precip = weather_by_city.get(city, (None, None, None, None))
            
            file.write(f"City: {city}\n")
            if depreciation > 0:
                file.write(f"  Average depreciation: {depreciation:.2f}% (value decreased)\n")
            elif depreciation < 0:
                file.write(f"  Average depreciation: {depreciation:.2f}% (value increased)\n")
            else:
                file.write(f"  Average depreciation: {depreciation:.2f}% (value remained the same)\n")
            file.write(f"  Average price of a new car: ${avg_price_new:.2f}\n")
            file.write(f"  Average price of a 6-year-old car: ${avg_price_old:.2f}\n")
            
            if avg_temp is not None:
                file.write(f"  Average temperature: {avg_temp:.2f}°C\n")
            if avg_humidity is not None:
                file.write(f"  Average humidity: {avg_humidity:.2f}%\n")
            if avg_windspeed is not None:
                file.write(f"  Average windspeed: {avg_windspeed:.2f} m/s\n")
            if avg_precip is not None:
                file.write(f"  Average precipitation: {avg_precip:.2f} mm\n")
            
            file.write("\n")

if __name__ == "__main__":
    main()


