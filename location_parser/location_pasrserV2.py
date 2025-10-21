import pandas as pd
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
from pymongo import MongoClient, errors as mongo_errors, UpdateOne
import time
import numpy as np

# КОНФІГУРАЦІЯ MONGODB
MONGO_URI = "mongodb://localhost:27017/" 
DB_NAME = "osint_course_db"
COLLECTION_LOC = "azs_locations"

# КОНФІГУРАЦІЯ ПРОЄКТУ
TARGET_CITIES = [
    "Хмельницький, Україна",
    "Кам'янець-Подільський, Україна",
    "Шепетівка, Україна",
    "Нетішин, Україна",
    "Старокостянтинів, Україна"
]
AZS_BRANDS = [
    "ОККО", "WOG", 
    "Shell", "Шелл", "Avantage 7", "Avantage7", 
    "Укрнафта", "Авіас", "VST", 
    "Укр-петроль", "АЗС Маркет", "AMIC Energy"
]

# ФУНКЦІЇ MONGODB

def get_db_connection():
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        print(f"Підключення до MongoDB успішне.")
        return client[DB_NAME]
    except Exception as e:
        print(f"Помилка підключення до MongoDB: {e}")
        raise ConnectionError("Неможливо підключитися до бази даних MongoDB.")

def setup_database_indexes(db):
    db[COLLECTION_LOC].create_index([("brand", 1), ("latitude", 1), ("longitude", 1)], 
                                    unique=True, 
                                    name='unique_location')
    print(f"Унікальний індекс для колекції '{COLLECTION_LOC}' створено/перевірено.")

# ЛОГІКА ПАРСЕРА

def run_geo_parser_to_db(db):
    
    geolocator = Nominatim(user_agent="OSINT_CourseWork_GeoParser")
    locations_data = []

    print(f"\nПочаток збору даних у {len(TARGET_CITIES)} містах...")

    for city in TARGET_CITIES:
        for brand in AZS_BRANDS:

            search_queries = [
                f"АЗС {brand} {city}",
                f"заправка {brand} {city}"
            ]
            
            for query in search_queries:
                try:
                    time.sleep(1)
                    locations = geolocator.geocode(query, exactly_one=False, limit=5, timeout=20)
                    
                    if locations:
                        for loc in locations:
                            locations_data.append({
                                'brand': brand,
                                'city': city.split(',')[0].strip(),
                                'address': loc.address,
                                'latitude': round(loc.latitude, 6),
                                'longitude': round(loc.longitude, 6)
                            })
                            
                except GeocoderTimedOut:
                    print(f" TimeOut для {query}. Пропуск.")
                except Exception:
                    pass 

    if not locations_data:
        print("Дані локацій не зібрано.")
        return 0
    
    # Видалення дублікатів
    df_locations = pd.DataFrame(locations_data).drop_duplicates(subset=['latitude', 'longitude', 'brand'])
    
    # ЗАПИС У MONGODB
    operations = []
    
    for doc in df_locations.to_dict('records'):
        filter_doc = {
            'brand': doc['brand'], 
            'latitude': doc['latitude'], 
            'longitude': doc['longitude']
        }
        operations.append(
            UpdateOne(filter_doc, {'$set': doc}, upsert=True)
        )

    try:
        result = db[COLLECTION_LOC].bulk_write(operations)
        print(f"\nЗавершено запис локацій до '{COLLECTION_LOC}'.")
        print(f"Додано (Upserted): {result.upserted_count}, Оновлено (Modified): {result.modified_count}, Всього операцій: {len(operations)}.")
    except mongo_errors.BulkWriteError as bwe:
        print(f"Помилка масового запису: {bwe.details}")
        
    return len(df_locations)

# ГОЛОВНА ФУНКЦІЯ

if __name__ == "__main__":
    
    try:
        db = get_db_connection()
        
        setup_database_indexes(db)
        
        total_locations = run_geo_parser_to_db(db)
        
        print(f"\nВсього записано унікальних АЗС: {total_locations}")
        
    except ConnectionError:
        print("\nВиконання зупинено через помилку підключення до MongoDB.")