import sqlite3

def check_duplicates(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Assuming the table name is 'cars' and it has a column 'id' as a unique identifier
    cursor.execute('''
        SELECT car_id, city_id, price, COUNT(*)
        FROM prices
        GROUP BY car_id, city_id, price
        HAVING COUNT(*) > 1
    ''')

    duplicates = cursor.fetchall()

    if duplicates:
        print("Duplicate records found:")
        for row in duplicates:
            print(f"Car ID: {row[0]}, City ID: {row[1]}, Price: {row[2]}, Count: {row[3]}")
    else:
        print("No duplicate records found.")

    conn.close()


def count_prices_by_car_and_city(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Count the number of prices for each car in each city
    cursor.execute('''
        SELECT car_id, city_id, COUNT(*)
        FROM prices
        GROUP BY car_id, city_id
    ''')

    counts = cursor.fetchall()

    if counts:
        print("Number of prices scraped for each car in each city:")
        for row in counts:
            print(f"Car ID: {row[0]}, City ID: {row[1]}, Count: {row[2]}")
    else:
        print("No prices found.")

    conn.close()

if __name__ == "__main__":
    db_path = '/Users/ryanzimmel/Desktop/SI 206/Final/car_data.db'
    check_duplicates(db_path)
    count_prices_by_car_and_city(db_path)