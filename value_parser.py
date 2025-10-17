import pandas as pd
from datetime import datetime
import requests
import io
import time

# --- КОНФІГУРАЦІЯ ---
TARGET_URL = "https://index.minfin.com.ua/ua/markets/fuel/detail/"
TARGET_REGION_UA = "Хмельницька обл." 
OUTPUT_FILE = "fuel_prices_khmelnytskyi.csv"
TODAY_DATE = datetime.now().strftime("%Y-%m-%d")

# Очікувані заголовки для таблиці Minfin.
# Тут ми примусово задаємо їх, ігноруючи складний MultiIndex
# Brand | Brand_Col_2 (Empty) | A-95_plus | A-95 | A-92 | DP | GAS
MINFIN_HEADERS = ['Brand', 'Drop_Col_2', 'A-95_plus', 'A-95', 'A-92', 'DP', 'GAS']

# --- ФУНКЦІЯ ПАРСИНГУ ---

def parse_price(value):
    """Очищує значення ціни (з рядка в float), обробляючи 'None' та порожні значення."""
    if not isinstance(value, str):
        return None
    try:
        # Видаляємо все зайве та замінюємо кому на крапку
        cleaned = value.strip().split()[0].replace(',', '.').replace('\xa0', '')
        if not cleaned:
            return None
        return float(cleaned)
    except:
        return None


def scrape_fuel_prices_detailed(url, region):
    """
    Завантажує HTML, парсить таблицю 'zebra', фільтрує за областю та примусово задає заголовки.
    """
    print(f"Початок парсингу цін для {region} з {url}...")
    
    try:
        # 1. Завантаження HTML-вмісту
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()

        # 2. Використання pandas.read_html
        # Встановлюємо header=None, щоб отримати рядок із заголовками як перший рядок даних,
        # а потім примусово призначимо наші заголовки.
        dfs = pd.read_html(io.StringIO(response.text), 
                           attrs={'class': 'zebra'},
                           thousands='.', decimal=',',
                           header=None,
                           flavor='lxml')
        
        if not dfs:
            print(f"❌ Не знайдено таблиці цін.")
            return pd.DataFrame()

        main_df = dfs[0] 
        
        # Видаляємо перші два рядки, які є фактичними заголовками таблиці
        main_df = main_df.iloc[2:]
        
        # 3. Примусове призначення заголовків
        if len(main_df.columns) == len(MINFIN_HEADERS):
            main_df.columns = MINFIN_HEADERS
            main_df = main_df.drop(columns=['Drop_Col_2']) # Видаляємо зайву пусту колонку
        else:
            print(f"❌ Помилка: Кількість колонок не відповідає очікуваній структурі.")
            return pd.DataFrame()


        # 4. Фільтрація за регіоном (Хмельницька обл.)
        
        # Знаходимо індекс рядка-заголовка цільового регіону (видаляємо посилання HTML)
        main_df['Brand_Clean'] = main_df['Brand'].astype(str).str.replace(r'<[^>]*>', '', regex=True).str.strip()
        
        region_start_index = main_df[
            main_df['Brand_Clean'].str.contains(region, na=False)
        ].index.min()
        
        if pd.isna(region_start_index):
            print(f"❌ Не знайдено розділу для регіону: {region}")
            return pd.DataFrame()

        # Знаходимо індекс наступної області (це буде кінець нашого блоку)
        # Шукаємо наступний рядок, який містить 'обл.' після нашого регіону
        next_region_df = main_df.loc[region_start_index+1:]
        next_region_index = next_region_df[
            next_region_df['Brand_Clean'].str.contains('обл.', na=False)
        ].index.min()
        
        # Виділяємо потрібний нам блок даних
        if pd.isna(next_region_index):
             # Якщо це остання область у таблиці
             region_df = main_df.loc[region_start_index+1:].copy()
        else:
             region_df = main_df.loc[region_start_index+1 : next_region_index-1].copy()

        # Видаляємо рядки, де ціни відсутні (у заголовках регіонів)
        region_df = region_df[~region_df['Brand'].astype(str).str.contains('href', na=False)]

        # 5. Фінальна трансформація та очищення
        region_df['Date'] = TODAY_DATE
        region_df['Region'] = region
        
        # Трансформуємо з широкого формату в довгий
        price_columns = ['A-95', 'DP', 'GAS', 'A-95_plus', 'A-92']
        price_cols_present = [c for c in price_columns if c in region_df.columns]
        
        melted_df = region_df.melt(
            id_vars=['Date', 'Region', 'Brand'],
            value_vars=price_cols_present,
            var_name='Fuel_Type',
            value_name='Price_Raw'
        )
        
        # 6. Очищення цін
        melted_df['Price'] = melted_df['Price_Raw'].apply(parse_price)
        final_df = melted_df.dropna(subset=['Price'])
        
        # Видаляємо зайві колонки
        final_df = final_df.drop(columns=['Price_Raw'])

        return final_df[['Date', 'Region', 'Brand', 'Fuel_Type', 'Price']]

    except requests.exceptions.RequestException as e:
        print(f"❌ Помилка підключення до сайту: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"❌ Невідома помилка парсингу: {e}. Спробуйте перевірити вручну структуру.")
        return pd.DataFrame()

# --- ВИКОНАННЯ ТА ЗБЕРЕЖЕННЯ ---
final_prices_df = scrape_fuel_prices_detailed(TARGET_URL, TARGET_REGION_UA)

if not final_prices_df.empty:
    # ... (блок збереження залишається без змін)
    try:
        existing_df = pd.read_csv(OUTPUT_FILE)
        df_final = pd.concat([existing_df, final_prices_df], ignore_index=True)
    except FileNotFoundError:
        df_final = final_prices_df
    
    df_final = df_final.drop_duplicates(subset=['Date', 'Region', 'Brand', 'Fuel_Type'], keep='last')
    df_final.to_csv(OUTPUT_FILE, index=False, encoding='utf-8')
    print(f"\n✅ Зібрано {len(final_prices_df)} записів цін для {TARGET_REGION_UA}.")
    print(f"✅ Усі ціни збережено/додано у файл: {OUTPUT_FILE}")
else:
    print("\n⚠️ Не вдалося зібрати дані про ціни. Файл не оновлено.")