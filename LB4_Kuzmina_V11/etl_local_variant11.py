import pandas as pd
from sqlalchemy import create_engine, text

# === CONFIG ===
DB_URL = "postgresql://airflow:airflow@localhost:5432/airflow"
DATA_PATH = "dags/data/"  # путь к CSV у тебя в проекте

engine = create_engine(DB_URL)


def load_csv(table_name, filename):
    print(f"→ Loading {filename} into {table_name}...")
    df = pd.read_csv(DATA_PATH + filename)
    df.to_sql(table_name, engine, if_exists="replace", index=False)
    print(f"✔ Loaded {len(df)} rows into {table_name}")


def create_datamart():
    print("→ Creating view flight_delays_datamart...")

    create_view_sql = """
    CREATE OR REPLACE VIEW flight_delays_datamart AS
    SELECT
        EXTRACT(MONTH FROM fl_date) AS month,
        airline,
        origin AS origin_airport,
        CASE WHEN arr_delay > 0 THEN 1 ELSE 0 END AS is_delayed,
        arr_delay AS arrival_delay_minutes
    FROM flight_delays_raw;
    """

    with engine.connect() as conn:
        conn.execute(text(create_view_sql))

    print("✔ View created.")


def main():
    print("=== VARIANT 11 ETL START ===")

    # Таблицы загружаем из готовых CSV:
    load_csv("flight_delays_raw", "flight_delays.csv")
    load_csv("airlines", "airlines.csv")
    load_csv("airports", "airports.csv")
    load_csv("flights", "flights.csv")

    # Создаем витрину
    create_datamart()

    print("=== ETL COMPLETE ===")


if __name__ == "__main__":
    main()
