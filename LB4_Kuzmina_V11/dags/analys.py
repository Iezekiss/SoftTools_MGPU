import pandas as pd
import sqlite3
import matplotlib.pyplot as plt

# ---- 1. Загружаем данные ----
df = pd.read_csv("./data/flight_delays.csv")

# Проверяем, есть ли колонка arrival_delay_minutes
if "arrival_delay_minutes" not in df.columns:
    df["arrival_delay_minutes"] = df["arrival_delay"]

# Добавляем индикатор
df["is_delayed"] = (df["arrival_delay_minutes"] > 0).astype(int)

# ---- 2. БД SQLite ----
conn = sqlite3.connect("./flights.db")
df.to_sql("flights", conn, if_exists="replace", index=False)

# ---- 3. Создаём VIEW ----
create_view_sql = """
CREATE VIEW IF NOT EXISTS v_flight_delays AS
SELECT
    month,
    airline,
    origin_airport,
    is_delayed,
    arrival_delay_minutes
FROM flights;
"""
conn.execute(create_view_sql)

# ---- 4. Запросы ----

total_delay = pd.read_sql("""
SELECT ROUND(AVG(is_delayed)*100,2) AS total_delay_percent
FROM v_flight_delays;
""", conn)
print("\nОбщий % задержек:\n", total_delay, "\n")


delay_by_airline = pd.read_sql("""
SELECT airline,
       ROUND(AVG(is_delayed)*100,2) AS delay_percent
FROM v_flight_delays
GROUP BY airline
ORDER BY delay_percent DESC;
""", conn)


flights_by_airline = pd.read_sql("""
SELECT airline,
       COUNT(*) AS flights
FROM v_flight_delays
GROUP BY airline
ORDER BY flights DESC;
""", conn)


combo = pd.read_sql("""
SELECT
    month,
    COUNT(*) AS flights,
    ROUND(AVG(is_delayed)*100,2) AS delay_percent
FROM v_flight_delays
GROUP BY month
ORDER BY month;
""", conn)


avg_delay = pd.read_sql("""
SELECT
    month,
    ROUND(AVG(arrival_delay_minutes),2) AS avg_delay
FROM v_flight_delays
GROUP BY month
ORDER BY month;
""", conn)

# ---- 5. Графики ----

# 1. % задержек по airline
plt.figure(figsize=(12,5))
plt.bar(delay_by_airline["airline"], delay_by_airline["delay_percent"])
plt.title("% задержек по авиакомпаниям")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("delay_by_airline.png")
plt.close()

# 2. Доля рейсов (круговая)
plt.figure(figsize=(8,8))
plt.pie(
    flights_by_airline["flights"],
    labels=flights_by_airline["airline"],
    autopct="%1.1f%%"
)
plt.title("Доля рейсов по авиакомпаниям")
plt.savefig("flights_share_airline.png")
plt.close()

# 3. Комбо график: кол-во рейсов + % задержек
plt.figure(figsize=(12,5))
plt.bar(combo["month"], combo["flights"], label="Кол-во рейсов")
plt.plot(combo["month"], combo["delay_percent"], color="red",
         marker="o", linewidth=2, label="% задержек")
plt.title("Рейсы и % задержек по месяцам")
plt.legend()
plt.tight_layout()
plt.savefig("combo_month.png")
plt.close()

# 4. Средняя задержка
plt.figure(figsize=(12,5))
plt.plot(avg_delay["month"], avg_delay["avg_delay"], marker="o")
plt.title("Средняя задержка по месяцам")
plt.xlabel("Месяц")
plt.ylabel("Минуты")
plt.tight_layout()
plt.savefig("avg_delay_month.png")
plt.close()

print("Готово: графики сохранены в папке проекта.")
