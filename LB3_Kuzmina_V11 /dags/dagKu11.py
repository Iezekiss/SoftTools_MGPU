from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
import pandas as pd
import sqlite3
import os

# -----------------------------------------------------------
# 1. Настройки и пути
# -----------------------------------------------------------
BASE_DIR = "/opt/airflow/kuzmina11"          # директория проекта
DATA_DIR = os.path.join(BASE_DIR, "data")    # входные данные
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
REPORTS_DIR = os.path.join(BASE_DIR, "reports")
DB_PATH = os.path.join(OUTPUT_DIR, "top_categories.db")

# создаём только если можно — без краша при отсутствии прав
for path in [OUTPUT_DIR, REPORTS_DIR]:
    try:
        os.makedirs(path, exist_ok=True)
    except PermissionError:
        print(f"⚠️ Нет прав на создание {path}, пропускаю.")

# -----------------------------------------------------------
# 2. Extract — загрузка данных из трёх источников
# -----------------------------------------------------------
def extract_data(**kwargs):
    csv_path = os.path.join(DATA_DIR, "stores.csv")
    xlsx_path = os.path.join(DATA_DIR, "categories.xlsx")
    json_path = os.path.join(DATA_DIR, "sales.json")

    # проверки наличия файлов
    for f in [csv_path, xlsx_path, json_path]:
        if not os.path.exists(f):
            raise FileNotFoundError(f"Файл не найден: {f}")

    stores = pd.read_csv(csv_path)
    categories = pd.read_excel(xlsx_path)
    sales = pd.read_json(json_path)

    # передача данных в XCom
    kwargs['ti'].xcom_push(key='stores', value=stores.to_json())
    kwargs['ti'].xcom_push(key='categories', value=categories.to_json())
    kwargs['ti'].xcom_push(key='sales', value=sales.to_json())

    print(f"✅ Данные успешно загружены из {DATA_DIR}")

# -----------------------------------------------------------
# 3. Transform — объединение и аналитика
# -----------------------------------------------------------
def transform_data(**kwargs):
    ti = kwargs['ti']
    stores = pd.read_json(ti.xcom_pull(key='stores'))
    categories = pd.read_json(ti.xcom_pull(key='categories'))
    sales = pd.read_json(ti.xcom_pull(key='sales'))

    merged = sales.merge(stores, on='store_id').merge(categories, on='category_id')

    # агрегируем выручку по категориям и городам
    summary = merged.groupby(['city', 'category_name'])['revenue'].sum().reset_index()

    # выбираем топовую категорию в каждом городе
    top_categories = summary.loc[
        summary.groupby('city')['revenue'].idxmax()
    ].reset_index(drop=True)

    ti.xcom_push(key='top_categories', value=top_categories.to_json())

    print("✅ Аналитика завершена, получены топ-категории:")
    print(top_categories.head())

# -----------------------------------------------------------
# 4. Load — сохранение в базу SQLite
# -----------------------------------------------------------
def load_to_sqlite(**kwargs):
    ti = kwargs['ti']
    top_categories = pd.read_json(ti.xcom_pull(key='top_categories'))

    try:
        conn = sqlite3.connect(DB_PATH)
        top_categories.to_sql('top_categories_by_city', conn, if_exists='replace', index=False)
        conn.close()
        print(f"✅ Результаты записаны в базу: {DB_PATH}")
    except Exception as e:
        print("⚠️ Ошибка при записи в базу:", e)

# -----------------------------------------------------------
# 5. Report — создание текстового отчёта
# -----------------------------------------------------------
def create_report():
    if not os.path.exists(DB_PATH):
        print(f"⚠️ База данных {DB_PATH} не найдена.")
        return

    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM top_categories_by_city", conn)
    conn.close()

    report_path = os.path.join(REPORTS_DIR, "report_top_categories.txt")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("ОТЧЁТ О ПРИБЫЛЬНЫХ КАТЕГОРИЯХ\n\n")
        for _, row in df.iterrows():
            f.write(f"Город: {row['city']} — Категория: {row['category_name']} — Выручка: {row['revenue']}\n")

    print(f"✅ Отчёт создан: {report_path}")

# -----------------------------------------------------------
# 6. Настройка DAG и задач
# -----------------------------------------------------------
default_args = {
    'owner': 'kuzmina11',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='dagKu11',
    default_args=default_args,
    description='Определение самой прибыльной категории товаров по городам (вариант 11)',
    schedule_interval=None,
    start_date=datetime(2025, 11, 8),
    catchup=False,
    tags=['variant11', 'kuzmina'],
) as dag:

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        provide_context=True
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True
    )

    load_task = PythonOperator(
        task_id='load_to_sqlite',
        python_callable=load_to_sqlite,
        provide_context=True
    )

    report_task = PythonOperator(
        task_id='create_report',
        python_callable=create_report
    )

    notify_task = EmailOperator(
        task_id='notify',
        to='student@example.com',
        subject='DAG dagKu11 успешно выполнен',
        html_content='ETL-процесс Kuzmina11 завершён без ошибок.'
    )

    extract_task >> transform_task >> load_task >> report_task >> notify_task