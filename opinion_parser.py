import pandas as pd
from geopy.distance import great_circle
from scipy.spatial import KDTree
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# --- КОНФІГУРАЦІЯ ФАЙЛІВ ---
LOC_FILE = "azs_locations_khmelnytskyi_oblast.csv"
PRICE_FILE = "fuel_prices_khmelnytskyi.csv"
N_NEIGHBORS = 3 # Кількість найближчих конкурентів для аналізу
OUTPUT_COMPETITION_FILE = "fuel_competition_index.csv"

# --- 1. ЗАВАНТАЖЕННЯ ДАНИХ ---
try:
    df_loc = pd.read_csv(LOC_FILE)
    df_price = pd.read_csv(PRICE_FILE)
    print("✅ Усі файли завантажено успішно.")
except FileNotFoundError as e:
    print(f"❌ Помилка: Файл не знайдено - {e}. Переконайтеся, що модулі 1-2 були запущені.")
    exit()

# --- 2. ПРОСТОРОВИЙ OSINT: РОЗРАХУНОК КЩ ---

# Підготовка даних для KDTree (Швидкий пошук найближчих сусідів)
# KDTree працює з широтою/довготою, але відстань розраховуємо через great_circle
coords = df_loc[['Latitude', 'Longitude']].values
tree = KDTree(coords)
competition_data = []

print(f"\nПочаток розрахунку Індексу Конкурентної Щільності (КЩ) для {len(df_loc)} АЗС...")

for i, row in df_loc.iterrows():
    # Знаходимо N_NEIGHBORS+1 найближчих точок (включаючи саму точку)
    # distance - Евклідова відстань, indices - індекси в масиві coords
    distances, indices = tree.query(row[['Latitude', 'Longitude']].values, k=N_NEIGHBORS + 1)
    
    # Фільтруємо саму точку (перший елемент з відстанню 0)
    neighbor_indices = indices[1:]
    
    # Розрахунок реальної відстані в кілометрах (great_circle)
    real_distances = []
    
    # Якщо сусідів менше, ніж N_NEIGHBORS (наприклад, у віддаленому селі)
    if len(neighbor_indices) == 0:
        avg_distance = 999.0 # Максимально високе значення
    else:
        for idx in neighbor_indices:
            # Обчислюємо відстань між поточною АЗС та сусідом
            dist = great_circle((row['Latitude'], row['Longitude']), 
                                (df_loc.iloc[idx]['Latitude'], df_loc.iloc[idx]['Longitude'])).km
            real_distances.append(dist)
        
        avg_distance = np.mean(real_distances)

    competition_data.append({
        'ID': row['ID'],
        'Brand': row['Brand'],
        'City': row['City'],
        'Avg_Competitor_Distance_km': round(avg_distance, 3)
    })

df_competition = pd.DataFrame(competition_data)

# Створення Індексу Конкурентної Щільності (КЩ):
# Низька відстань = Висока щільність/конкуренція
max_dist = df_competition['Avg_Competitor_Distance_km'].max()
df_competition['Competition_Index'] = 1 - (df_competition['Avg_Competitor_Distance_km'] / max_dist)

# --- 3. КРОС-АНАЛІЗ (ЦІНА VS КОНКУРЕНЦІЯ) ---

# Розрахунок середньої ціни для A-95
df_price_a95 = df_price[df_price['Fuel_Type'] == 'A-95'][['Brand', 'Price']]
df_price_summary = df_price_a95.groupby('Brand')['Price'].mean().reset_index()
df_price_summary = df_price_summary.rename(columns={'Price': 'Avg_Price_A95'})

# Об'єднання з конкурентною щільністю (середня КЩ за брендом)
df_comp_summary = df_competition.groupby('Brand')['Competition_Index'].mean().reset_index()
df_comp_summary = df_comp_summary.rename(columns={'Competition_Index': 'Avg_Competition_Index'})

df_cross = df_comp_summary.merge(df_price_summary, on='Brand', how='inner')

# Розрахунок кореляції
correlation = df_cross['Avg_Price_A95'].corr(df_cross['Avg_Competition_Index'])
print(f"\nКоефіцієнт кореляції (Ціна A-95 vs Індекс КЩ): {correlation:.2f}")

# Збереження результатів
df_competition.to_csv(OUTPUT_COMPETITION_FILE, index=False, encoding='utf-8')
print(f"✅ Результати КЩ збережено у файл: {OUTPUT_COMPETITION_FILE}")

# --- 4. ВІЗУАЛІЗАЦІЯ (ДЛЯ КУРСОВОЇ) ---

plt.figure(figsize=(15, 6))

# Графік 1: Середня конкурентна щільність за брендом
plt.subplot(1, 2, 1)
sns.barplot(x='Brand', y='Avg_Competition_Index', data=df_cross, palette='magma')
plt.title('Середній Індекс Конкурентної Щільності (КЩ) за Брендом')
plt.ylabel('Індекс КЩ (0 - низька, 1 - висока)')
plt.xlabel('Бренд')
plt.xticks(rotation=45, ha='right')
plt.grid(axis='y', alpha=0.5)

# Графік 2: Діаграма розсіювання (Ціна vs КЩ)
plt.subplot(1, 2, 2)
sns.regplot(x='Avg_Price_A95', y='Avg_Competition_Index', data=df_cross, 
            scatter_kws={'s': 100}, line_kws={"color": "darkred"})
plt.title(f'Кореляція: Ціна A-95 vs Індекс КЩ (r={correlation:.2f})')
plt.xlabel('Середня Ціна A-95 (грн.)')
plt.ylabel('Середній Індекс КЩ')
plt.grid(alpha=0.5)

plt.tight_layout()
plt.show()

# --- ФІНАЛЬНИЙ ВИСНОВОК ---
print("\n=== ВИСНОВКИ ДЛЯ КУРСОВОЇ (Приклад) ===")
print(f"1. Знайдено кореляцію (r={correlation:.2f}) між ціною та конкурентною щільністю.")
print("2. Бренди з низьким Індексом КЩ, ймовірно, розташовані у менш насичених районах, що може свідчити про стратегію захоплення нових ринків.")
print("3. Бренди з високою ціною та високим Індексом КЩ (наприклад, WOG/ОККО) розташовані у 'преміум'-локаціях, де вони можуть підтримувати високі ціни завдяки бренду, незважаючи на конкуренцію.")