from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# -----------------------------
# Простая функция для проверки
# -----------------------------
def hello_airflow():
    print("✅ Airflow DAG работает! Всё подключено корректно.")

# -----------------------------
# Параметры по умолчанию
# -----------------------------
default_args = {
    'owner': 'kuzmina',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

# -----------------------------
# Определение DAG
# -----------------------------
with DAG(
    dag_id='test_dag',
    default_args=default_args,
    description='Проверочный DAG для отладки Airflow',
    schedule_interval=None,
    start_date=datetime(2025, 11, 8),
    catchup=False,
    tags=['debug', 'check'],
) as dag:

    test_task = PythonOperator(
        task_id='hello_airflow',
        python_callable=hello_airflow
    )

    test_task