import pandas as pd
from pymongo import MongoClient
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import numpy as np

MONGO_URI = "mongodb://localhost:27017/" 
DB_NAME = "osint_course_db"
COLLECTION_PRICE = "fuel_prices"

FUEL_VISUAL_ORDER = ['A-95_plus', 'A-95', 'A-92', 'DP', 'GAS']
UKR_LEGEND_MAP = {
    'A-95_plus': 'А-95+', 
    'A-95': 'А-95', 
    'A-92': 'А-92', 
    'DP': 'ДП', 
    'GAS': 'Газ'
}

def visualize_all_fuel_prices_mongo():
    print("Підключення до MongoDB та зчитування даних...")
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        db = client[DB_NAME]
        
        price_cursor = db[COLLECTION_PRICE].find({}, {'_id': 0, 'brand': 1, 'fuel_type': 1, 'price': 1})
        df_price = pd.DataFrame(list(price_cursor))

        if df_price.empty:
            print("Немає даних про ціни у колекції MongoDB.")
            return

        df_avg_prices = df_price.groupby(['brand', 'fuel_type'])['price'].mean().reset_index()
        df_avg_prices = df_avg_prices.rename(columns={'price': 'Avg_Price'})
        
        if df_avg_prices.empty:
            print("Не знайдено цін для візуалізації.")
            return

        df_avg_prices = df_avg_prices[df_avg_prices['fuel_type'].isin(FUEL_VISUAL_ORDER)].copy()
        brand_order = df_avg_prices[df_avg_prices['fuel_type'] == 'A-95'].sort_values(by='Avg_Price', ascending=False)['brand'].tolist()
        
        print(f"Обчислено середні ціни для {len(df_avg_prices)} комбінацій Бренд/Паливо.")
        
        plt.figure(figsize=(16, 8))
        sns.barplot(
            x='brand', 
            y='Avg_Price', 
            hue='fuel_type', 
            data=df_avg_prices, 
            order=brand_order, 
            hue_order=FUEL_VISUAL_ORDER,
            palette='tab10'
        )
        
        plt.title('Середня Ціна Пального за Брендом та Типом (Хмельницька Область)', fontsize=16)
        plt.xlabel('Бренд АЗС', fontsize=12)
        plt.ylabel('Середня Ціна (грн.)', fontsize=12)
        plt.xticks(rotation=45, ha='right')

        h, l = plt.gca().get_legend_handles_labels()
        new_labels = [UKR_LEGEND_MAP[label] for label in FUEL_VISUAL_ORDER]
        plt.legend(h, new_labels, title='Тип Пального')

        plt.grid(axis='y', alpha=0.6)
        plt.tight_layout()
        plt.show()

    except Exception as e:
        print(f"Критична помилка візуалізації: {e}")

if __name__ == '__main__':
    visualize_all_fuel_prices_mongo()
