import pandas as pd
import random
import os

# Базовая директория
BASE_DIR = "kuzmina11"
DATA_DIR = os.path.join(BASE_DIR, "data")

# Создаём папку, если её нет
os.makedirs(DATA_DIR, exist_ok=True)

# 1. Магазины
cities = ['Moscow', 'Kazan', 'Novosibirsk', 'Sochi', 'Ekaterinburg']
stores = pd.DataFrame({
    'store_id': range(1, 21),
    'city': [random.choice(cities) for _ in range(20)]
})
stores_path = os.path.join(DATA_DIR, 'stores.csv')
stores.to_csv(stores_path, index=False)

# 2. Категории
categories = pd.DataFrame({
    'category_id': range(1, 6),
    'category_name': ['Electronics', 'Clothing', 'Toys', 'Furniture', 'Books']
})
categories_path = os.path.join(DATA_DIR, 'categories.xlsx')
categories.to_excel(categories_path, index=False)

# 3. Продажи
sales = pd.DataFrame({
    'store_id': [random.randint(1, 20) for _ in range(200)],
    'category_id': [random.randint(1, 5) for _ in range(200)],
    'revenue': [random.randint(500, 5000) for _ in range(200)]
})
sales_path = os.path.join(DATA_DIR, 'sales.json')
sales.to_json(sales_path, orient='records', indent=2)

print("✅ Данные успешно сгенерированы!")
print(f"Папка: {os.path.abspath(DATA_DIR)}")
print(f"- {stores_path}")
print(f"- {categories_path}")
print(f"- {sales_path}")