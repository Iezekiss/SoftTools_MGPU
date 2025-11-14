import pandas as pd
import json
import matplotlib.pyplot as plt

# Читаем CSV
df = pd.read_csv("flights.csv", low_memory=False)

# Нормализуем названия столбцов
df.columns = [c.strip().upper() for c in df.columns]

# Убедимся, что используем ОДИН ARRIVAL_DELAY
if "ARRIVAL_DELAY" not in df.columns:
    raise ValueError("В CSV нет столбца ARRIVAL_DELAY")

# Строим VIEW
df_view = pd.DataFrame({
    "month": df["MONTH"],
    "airline": df["AIRLINE"],
    "origin_airport": df["ORIGIN_AIRPORT"],
    "is_delayed": (df["ARRIVAL_DELAY"] > 0).astype(int),
    "arrival_delay_minutes": df["ARRIVAL_DELAY"]
})

# Сохраняем VIEW в JSON
df_view.to_json("view.json", orient="records")

# --- 1. Индикатор ---
delay_percent = df_view["is_delayed"].mean() * 100

plt.figure()
plt.bar(["Delay %"], [delay_percent])
plt.title("Индикатор: % задержек")
plt.savefig("indicator.png")

# --- 2. Столбчатая ---
delay_by_airline = df_view.groupby("airline")["is_delayed"].mean() * 100

plt.figure(figsize=(10,5))
delay_by_airline.sort_values().plot(kind="bar")
plt.title("% задержек по авиакомпаниям")
plt.savefig("delay_airline.png")

# --- 3. Круговая ---
share_by_airline = df_view["airline"].value_counts()

plt.figure(figsize=(8,8))
share_by_airline.plot(kind="pie", autopct='%1.1f%%')
plt.title("Доля рейсов по авиакомпаниям")
plt.ylabel("")
plt.savefig("share_airline.png")

# --- 4. Комбинированная ---
combo = df_view.groupby("month").agg({
    "is_delayed": "mean",
    "airline": "count"
})

combo["is_delayed"] *= 100

plt.figure(figsize=(10,5))
plt.bar(combo.index, combo["airline"], label="Кол-во рейсов")
plt.plot(combo.index, combo["is_delayed"], color="red", label="% задержек")
plt.xlabel("Месяц")
plt.title("Комбинированная: кол-во рейсов + % задержек")
plt.legend()
plt.savefig("combo.png")

# --- 5. Линейная ---
avg_delay_by_month = df_view.groupby("month")["arrival_delay_minutes"].mean()

plt.figure(figsize=(10,5))
avg_delay_by_month.plot()
plt.title("Средняя задержка по месяцам")
plt.savefig("avg_delay.png")

print("Готово! Созданы: view.json и 5 графиков.")
