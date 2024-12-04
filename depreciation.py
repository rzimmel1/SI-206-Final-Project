# calculates the depreciation of the cars in our database

import sqlite3
import re

def get_average_price_by_year_and_city(year, city_id):
    conn = sqlite3.connect('car_data.db')
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
    conn = sqlite3.connect('car_data.db')
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

if __name__ == "__main__":
    # Calculate and print average depreciation by city using all cars
    depreciation_by_city = calculate_average_depreciation_by_city()
    for city, data in depreciation_by_city.items():
        depreciation, avg_price_new, avg_price_old = data
        if depreciation > 0:
            print(f"Average depreciation in {city}: {depreciation:.2f}% (value decreased)")
        elif depreciation < 0:
            print(f"Average depreciation in {city}: {depreciation:.2f}% (value increased)")
        else:
            print(f"Average depreciation in {city}: {depreciation:.2f}% (value remained the same)")
        print(f"  Average price of a new car: ${avg_price_new:.2f}")
        print(f"  Average price of a 6-year-old car: ${avg_price_old:.2f}")

