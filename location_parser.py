import pandas as pd
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import folium
import time

# --- КОНФІГУРАЦІЯ (ОНОВЛЕНО ДЛЯ ВСІЄЇ ОБЛАСТІ) ---

# Список ключових міст Хмельницької області
TARGET_CITIES = [
    "Хмельницький, Україна",
    "Кам'янець-Подільський, Україна",
    "Шепетівка, Україна",
    "Нетішин, Україна",
    "Старокостянтинів, Україна"
]

# Розширений список брендів
AZS_BRANDS = [
    "ОККО", "WOG", "UPG", "Shell", 
    "БРСМ-Нафта", "AMIC Energy", "SOCAR", "Avantage 7", 
    "Укрнафта", "Marshal" # Додаємо Укрнафту та Marshal для повнішої картини
]

OUTPUT_CSV_FILE = "azs_locations_khmelnytskyi_oblast.csv"
OUTPUT_MAP_FILE = "azs_competitors_map_khmelnytskyi_oblast.html"

# Ініціалізація геокодера
geolocator = Nominatim(user_agent="OSINT_CourseWork_AZS_Finder_Oblast")
locations_data = []
unique_coordinates = set()

# --- ФУНКЦІЯ ГЕОКОДУВАННЯ ---

def get_multiple_locations(query, brand, city):
    """
    Намагається знайти до 10 релевантних локацій за запитом.
    """
    try:
        # Пауза для дотримання лімітів API (1 запит на секунду)
        time.sleep(1)
        
        # Шукаємо до 10 результатів, щоб зібрати максимум АЗС
        locations = geolocator.geocode(query, exactly_one=False, limit=10, timeout=15)
        
        if locations:
            for loc in locations:
                # Ключ унікальності: координати (для уникнення дублікатів)
                coord_key = (round(loc.latitude, 4), round(loc.longitude, 4))
                
                if coord_key not in unique_coordinates:
                    unique_coordinates.add(coord_key)
                    locations_data.append({
                        'Brand': brand,
                        'City': city.split(',')[0].strip(),
                        'Address': loc.address,
                        'Latitude': loc.latitude,
                        'Longitude': loc.longitude,
                        'Source_Query': query
                    })
                    # print(f"  Знайдено: {brand} в {city} за адресою: {loc.address[:30]}...")
        else:
            # print(f"Помилка: Координати для запиту '{query}' не знайдено.")
            pass
            
    except GeocoderTimedOut:
        print(f"Помилка: Час очікування запиту для '{query}' вичерпано. Пропуск.")
    except Exception as e:
        print(f"Невідома помилка при обробці '{query}': {e}")


# --- ОСНОВНИЙ ЦИКЛ ЗБОРУ ---

print(f"Початок збору даних про АЗС у {len(TARGET_CITIES)} містах Хмельницької області...")

for city in TARGET_CITIES:
    for brand in AZS_BRANDS:
        # Створення двох типів пошукових запитів для кожного міста/бренду
        search_queries = [
            f"АЗС {brand} {city}",
            f"заправка {brand} {city}"
        ]
        
        for query in search_queries:
            get_multiple_locations(query, brand, city)


# --- ОБРОБКА ТА ЗБЕРЕЖЕННЯ ---

df_locations = pd.DataFrame(locations_data)

if not df_locations.empty:
    df_locations = df_locations.drop_duplicates(subset=['Latitude', 'Longitude'])
    df_locations['ID'] = range(1, len(df_locations) + 1)
    
    # Збереження у CSV
    df_locations[['ID', 'Brand', 'City', 'Address', 'Latitude', 'Longitude']].to_csv(
        OUTPUT_CSV_FILE, index=False, encoding='utf-8'
    )
    print(f"\n✅ Зібрано {len(df_locations)} унікальних локацій АЗС у Хмельницькій області.")
    print(f"✅ Дані успішно збережено у файл: {OUTPUT_CSV_FILE}")
else:
    print("\n❌ Дані не зібрано. Перевірте підключення до інтернету та запити.")

# --- ВІЗУАЛІЗАЦІЯ (КАРТА) ---

if not df_locations.empty:
    # Визначення центру карти (приблизний центр області)
    # Середні координати Хмельницького ~ 49.42, 26.98
    center_lat = df_locations['Latitude'].mean() if len(df_locations) > 1 else 49.42
    center_lon = df_locations['Longitude'].mean() if len(df_locations) > 1 else 26.98
    
    m = folium.Map(location=[center_lat, center_lon], zoom_start=9) # Зум 9 підходить для області
    
    # Визначення кольорів для брендів
    brand_colors = {
        "ОККО": "darkgreen", "WOG": "blue", "UPG": "red", 
        "Shell": "orange", "БРСМ-Нафта": "lightred", "AMIC Energy": "darkblue",
        "SOCAR": "purple", "Avantage 7": "green", "Укрнафта": "lightgreen",
        "Marshal": "darkred", "Unknown": "gray"
    }
    
    # Додавання маркерів на карту
    for index, row in df_locations.iterrows():
        popup_html = f"<b>Бренд:</b> {row['Brand']} ({row['City']})<br><b>Адреса:</b> {row['Address']}"
        
        folium.Marker(
            [row['Latitude'], row['Longitude']], 
            popup=folium.Popup(popup_html, max_width=300), 
            tooltip=row['Brand'],
            icon=folium.Icon(color=brand_colors.get(row['Brand'], 'gray'), icon='info-sign')
        ).add_to(m)
        
    m.save(OUTPUT_MAP_FILE)
    print(f"✅ Інтерактивну карту (Хмельницька обл.) збережено у файл: {OUTPUT_MAP_FILE}")