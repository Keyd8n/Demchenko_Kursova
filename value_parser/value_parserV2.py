import pandas as pd
import requests
import io
import time
from datetime import datetime
from pymongo import MongoClient, errors as mongo_errors, UpdateOne
import numpy as np

# --- КОНФІГУРАЦІЯ MONGODB (ОБОВ'ЯЗКОВО ЗАМІНІТЬ!) ---
MONGO_URI = "mongodb://localhost:27017/" 
DB_NAME = "osint_course_db"
COLLECTION_PRICE = "fuel_prices"

# --- КОНФІГУРАЦІЯ ПАРСЕРА ---
TARGET_URL = "https://index.minfin.com.ua/ua/markets/fuel/detail/"
TARGET_REGION_UA = "Хмельницька обл." 

# Дата як рядок (для унікальності в MongoDB)
TODAY_DATE = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) 
TODAY_DATE_STR = TODAY_DATE.strftime("%Y-%m-%d") 

AZS_BRANDS = [
    "ОККО", "WOG", "Shell", "UPG", 
    "БРСМ-Нафта", "AMIC Energy", "SOCAR", "Укрнафта"
]
# Заголовки таблиці Minfin для примусового призначення
MINFIN_HEADERS = ['Brand', 'Drop_Col_2', 'A-95_plus', 'A-95', 'A-92', 'DP', 'GAS']


# --- ФУНКЦІЇ MONGODB ---

def get_db_connection():
    """Створює та повертає об'єкт бази даних MongoDB."""
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        print(f"✅ Підключення до MongoDB успішне.")
        return client[DB_NAME]
    except Exception as e:
        print(f"❌ Помилка підключення до MongoDB: {e}")
        raise ConnectionError("Неможливо підключитися до бази даних MongoDB.")

def setup_database_indexes(db):
    """Створює унікальний індекс для колекції цін."""
    
    # Використовуємо try-except для ігнорування конфліктів індексів (Код 85/86)
    try:
        # Індекс на полях (date_str, brand, fuel_type) для унікальності
        db[COLLECTION_PRICE].create_index([("date_str", 1), ("brand", 1), ("fuel_type", 1)], 
                                          unique=True, 
                                          name='price_day_unique_idx') # Нова, унікальна назва
        print(f"✅ Унікальний індекс для колекції '{COLLECTION_PRICE}' створено/перевірено.")
        
    except mongo_errors.OperationFailure as e:
        if e.code not in [85, 86]:
            raise e
        print("✅ Індекс цін вже існує (конфлікт індексів проігноровано).")
    except Exception as e:
        print(f"❌ Невідома помилка при створенні індексів: {e}")
        raise

# --- ФУНКЦІЇ ПАРСЕРА 2: ЦІНИ ТА ЗБЕРЕЖЕННЯ ---

def parse_price(value):
    """Очищує значення ціни (з рядка в float)."""
    if not isinstance(value, str):
        return None
    try:
        # Видаляємо пробіли, грн/л та замінюємо кому на крапку
        cleaned = value.strip().split()[0].replace(',', '.').replace('\xa0', '')
        if not cleaned: return None
        return float(cleaned)
    except:
        return None

def run_price_parser_to_db(db):
    """Збирає ціни з Minfin, масштабує їх та записує у колекцію fuel_prices."""
    print(f"\n[Парсер Цін] Початок парсингу цін для {TARGET_REGION_UA} з {TARGET_URL}...")
    
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
        response = requests.get(TARGET_URL, headers=headers, timeout=15)
        response.raise_for_status()

        dfs = pd.read_html(io.StringIO(response.text), attrs={'class': 'zebra'}, header=None, flavor='lxml')
        
        if not dfs: return 0

        # --- ЛОГІКА ПАРСИНГУ ТА ФІЛЬТРАЦІЇ ---
        main_df = dfs[0].iloc[2:]
        
        if len(main_df.columns) != len(MINFIN_HEADERS): return 0

        main_df.columns = MINFIN_HEADERS
        final_df = main_df.drop(columns=['Drop_Col_2']).copy()

        region_start_index = final_df[final_df['Brand'].astype(str).str.contains(TARGET_REGION_UA, na=False)].index.min()
        
        if pd.isna(region_start_index): return 0
        
        next_region_df = final_df.loc[region_start_index+1:]
        next_region_index = next_region_df[next_region_df['Brand'].astype(str).str.contains('обл.', na=False)].index.min()
        
        if pd.isna(next_region_index): region_df = final_df.loc[region_start_index+1:].copy()
        else: region_df = final_df.loc[region_start_index+1 : next_region_index-1].copy()
             
        # 2. Трансформація для MongoDB
        region_df['date_str'] = TODAY_DATE_STR 
        region_df['region'] = TARGET_REGION_UA
        
        price_columns = ['A-95', 'DP', 'GAS', 'A-95_plus', 'A-92']
        price_cols_present = [c for c in price_columns if c in region_df.columns]
        
        melted_df = region_df.melt(
            id_vars=['date_str', 'region', 'Brand'],
            value_vars=price_cols_present,
            var_name='fuel_type',
            value_name='price_raw'
        )
        
        melted_df['price'] = melted_df['price_raw'].apply(parse_price)
        
        final_db_df = melted_df.dropna(subset=['price']).rename(columns={'Brand': 'brand'}).copy()
        final_db_df = final_db_df[final_db_df['brand'].isin(AZS_BRANDS)].drop(columns=['price_raw'])

        # !!! ВИПРАВЛЕННЯ: МАСШТАБУВАННЯ ЦІНИ !!!
        final_db_df['price'] = final_db_df['price'].apply(lambda x: round(x / 100.0, 2))
        
    except Exception as e:
        print(f"❌ [Парсер Цін] Критична помилка парсингу: {e}")
        return 0

    # --- ЗАПИС У MONGODB (UPSERT) ---
    operations = []
    
    for doc in final_db_df.to_dict('records'):
        # Фільтр для UPSERT (унікальність: дата_рядок + бренд + тип палива)
        filter_doc = {'date_str': doc['date_str'], 'brand': doc['brand'], 'fuel_type': doc['fuel_type']}
        
        # Оновлення: оновлюємо ціну та регіон
        update_doc = {'$set': {'price': doc['price'], 'region': doc['region']}}
        
        operations.append(
            UpdateOne(filter_doc, update_doc, upsert=True)
        )
        
    try:
        result = db[COLLECTION_PRICE].bulk_write(operations)
        print(f"✅ [Парсер Цін] Завершено запис/оновлення цін. Додано: {result.upserted_count}, Оновлено: {result.modified_count}.")
        return len(final_db_df)
    except mongo_errors.BulkWriteError as bwe:
        print(f"  [DB Error] Помилка масового запису цін: {bwe.details}")
        return 0

# --- ГОЛОВНА ФУНКЦІЯ ВИКОНАННЯ ---

if __name__ == "__main__":
    
    try:
        db = get_db_connection()
        
        setup_database_indexes(db)
        
        run_price_parser_to_db(db)

        print("\n*** ПАРСЕР ЦІН ЗАВЕРШЕНО ***")
        
    except ConnectionError:
        print("\n⚠️ Виконання зупинено через помилку підключення до MongoDB.")