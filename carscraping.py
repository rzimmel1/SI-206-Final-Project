#file to scrape data from Kelley Blue Book Website

import requests
from bs4 import BeautifulSoup
import json
import re
import sqlite3

#https://www.kbb.com/cars-for-sale/all/2022/audi/a5/miami-fl?newSearch=true&searchRadius=100&zip=33101
def scrape_car_data(make, model, year, city, state, zip_code):
    url = f"https://www.kbb.com/cars-for-sale/all/{year}/{make}/{model}/{city}-{state}?newSearch=true&searchRadius=100&zip={zip_code}"
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Failed to load page {url}")
    
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Example: Extracting price (this will depend on the actual HTML structure)
    price_tags = soup.find_all('div', class_="text-size-600 text-ultra-bold first-price")
    prices = [price_tag.text.strip() for price_tag in price_tags if price_tag.text != '']
    
    return {
        'make': make,
        'model': model,
        'year': year,
        'prices': prices
    }



def setup_database():
    conn = sqlite3.connect('car_data.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS cities
                 (id INTEGER PRIMARY KEY, city TEXT, state TEXT, zip_code TEXT,
                  UNIQUE(city, state, zip_code))''')
    c.execute('''CREATE TABLE IF NOT EXISTS cars
                 (id INTEGER PRIMARY KEY, make TEXT, model TEXT, year INTEGER,
                  UNIQUE(make, model, year))''')
    c.execute('''CREATE TABLE IF NOT EXISTS prices
                 (id INTEGER PRIMARY KEY, car_id INTEGER, city_id INTEGER, price TEXT,
                  FOREIGN KEY(car_id) REFERENCES cars(id),
                  FOREIGN KEY(city_id) REFERENCES cities(id),
                  UNIQUE(car_id, city_id, price))''')
    conn.commit()
    conn.close()

def store_car_and_city(car_data, city, state, zip_code):
    conn = sqlite3.connect('car_data.db')
    c = conn.cursor()
    
    # Insert city data and get the city_id
    c.execute('''INSERT OR IGNORE INTO cities (city, state, zip_code) 
                 VALUES (?, ?, ?)''', (city, state, zip_code))
    c.execute('''SELECT id FROM cities WHERE city = ? AND state = ? AND zip_code = ?''',
              (city, state, zip_code))
    city_id = c.fetchone()[0]
    
    # Insert car data and get the car_id
    c.execute('''INSERT OR IGNORE INTO cars (make, model, year) 
                 VALUES (?, ?, ?)''', (car_data['make'], car_data['model'], car_data['year']))
    c.execute('''SELECT id FROM cars WHERE make = ? AND model = ? AND year = ?''',
              (car_data['make'], car_data['model'], car_data['year']))
    car_id = c.fetchone()[0]
    
    conn.commit()
    conn.close()
    
    return car_id, city_id

def store_prices(car_id, city_id, prices):
    conn = sqlite3.connect('car_data.db')
    c = conn.cursor()
    
    # Insert prices data
    for price in prices:
        c.execute('''INSERT OR IGNORE INTO prices (car_id, city_id, price) 
                     VALUES (?, ?, ?)''', (car_id, city_id, price))
    
    conn.commit()
    conn.close()

def main():
    setup_database()
    cars = [
        ('ford', 'f150', 2018),
        ('honda', 'civic', 2018),
        ('toyota', 'rav4', 2018),
        ('tesla', 'model-3', 2018),
        ('toyota', 'camry', 2018),
        ('ford', 'f150', 2024),
        ('honda', 'civic', 2024),
        ('toyota', 'rav4', 2024),
        ('tesla', 'model-3', 2023),
        ('toyota', 'camry', 2024),
    ]
    
    cities = [
        ('miami', 'fl', '33101'),
        ('phoenix', 'az', '85001'),
        ('seattle', 'wa', '98101'),
        ('minneapolis', 'mn', '55401'),
        ('aurora', 'co', '80019'),
    ]
    
    
    total_prices_added = 0
    max_new_prices_per_run = 25

    for make, model, year in cars:
        for city, state, zip_code in cities:
            # Check if we've already added 25 new prices
            if total_prices_added >= max_new_prices_per_run:
                print(f"Reached limit of {max_new_prices_per_run} new prices for this run.")
                return
            
            # Scrape car data
            car_data = scrape_car_data(make, model, year, city, state, zip_code)
            car_id, city_id = store_car_and_city(car_data, city, state, zip_code)
            
            # Store prices and track how many were added
            conn = sqlite3.connect('car_data.db')
            c = conn.cursor()
            for price in car_data['prices']:
                if total_prices_added >= max_new_prices_per_run:
                    break  # Stop if limit is reached
                
                c.execute('''INSERT OR IGNORE INTO prices (car_id, city_id, price) 
                             VALUES (?, ?, ?)''', (car_id, city_id, price))
                if c.rowcount > 0:  # Only count successfully added prices
                    total_prices_added += 1
            conn.commit()
            conn.close()

if __name__ == "__main__":
    main()

