import pandas as pd
from pymongo import MongoClient
import folium
import numpy as np

# КОНФІГУРАЦІЯ MONGODB
MONGO_URI = "mongodb://localhost:27017/" 
DB_NAME = "osint_course_db"
COLLECTION_LOC = "azs_locations"
OUTPUT_MAP_FILE = "azs_competitors_map_khmelnytskyi_oblast.html"

# ФУНКЦІЯ ПІДКЛЮЧЕННЯ

def get_db_collection():
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        return client[DB_NAME][COLLECTION_LOC]
    except Exception as e:
        print(f"Помилка підключення до MongoDB: {e}")
        return None

# ФУНКЦІЯ СТВОРЕННЯ КАРТИ

def create_map_from_mongo():
    
    collection = get_db_collection()
    if collection is None:
        print("Неможливо створити карту: Відсутнє підключення до БД.")
        return

    print("\nЧитання даних з MongoDB...")

    cursor = collection.find({}, {'_id': 0, 'brand': 1, 'city': 1, 'address': 1, 'latitude': 1, 'longitude': 1})

    df_locations = pd.DataFrame(list(cursor))
    
    if df_locations.empty:
        print("Немає даних локацій у колекції MongoDB для побудови карти.")
        return

    # АНАЛІЗ ЦЕНТРУ КАРТИ ТА ІНІЦІАЛІЗАЦІЯ МАРКЕРІВ
    center_lat = df_locations['latitude'].mean()
    center_lon = df_locations['longitude'].mean()
    
    m = folium.Map(location=[center_lat, center_lon], zoom_start=9)
    
    brand_colors = {
        "ОККО": "darkgreen", "WOG": "blue","Shell": "orange",
        "Шелл": "lightred", "Avantage 7": "green","Avantage7": "darkblue",
        "Укрнафта": "lightgreen","Авіас": "purple", "VST": "darkred",
        "Укр-петроль": "cadetblue","АЗС Маркет": "beige", "AMIC Energy": "pink"
    }
    
    for index, row in df_locations.iterrows():
        popup_html = f"""
            <b>Бренд:</b> {row['brand']} ({row['city']})<br>
            <b>Адреса:</b> {row['address']}<br>
            <small>Координати: ({row['latitude']:.4f}, {row['longitude']:.4f})</small>
        """
        folium.Marker(
            [row['latitude'], row['longitude']], 
            popup=folium.Popup(popup_html, max_width=300), 
            tooltip=row['brand'],
            icon=folium.Icon(color=brand_colors.get(row['brand'], 'gray'), icon='info-sign')
        ).add_to(m)

    m.save(OUTPUT_MAP_FILE)
    print(f"Інтерактивну карту ({len(df_locations)} локацій) збережено у файл: {OUTPUT_MAP_FILE}")
    print("Відкрийте файл у браузері, щоб побачити карту.")   

if __name__ == "__main__":
    create_map_from_mongo()