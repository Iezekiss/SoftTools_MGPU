import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

results_dir = Path(__file__).resolve().parent / "results"
merged = pd.read_csv(results_dir / "merged_dataset.csv")

plt.style.use("seaborn-v0_8-whitegrid")

# 1. Распределение общего дохода
plt.figure(figsize=(8, 5))
sns.histplot(merged["total_income"], bins=10, kde=True, color="skyblue")
plt.title("Распределение общего дохода студентов")
plt.xlabel("Общий доход (стипендия + призы)")
plt.ylabel("Количество студентов")
plt.tight_layout()
plt.savefig(results_dir / "income_distribution.png")

# 2. Средний доход по предметам
plt.figure(figsize=(8, 5))
sns.barplot(x="faculty", y="total_income", data=merged, ci=None, palette="crest")
plt.title("Средний общий доход по предметам")
plt.xlabel("Предмет")
plt.ylabel("Средний доход")
plt.xticks(rotation=30)
plt.tight_layout()
plt.savefig(results_dir / "income_by_faculty.png")

# 3. Зависимость дохода от года поступления
plt.figure(figsize=(8, 5))
sns.boxplot(x="enrollment_year", y="total_income", data=merged, palette="pastel")
plt.title("Распределение доходов по годам поступления")
plt.xlabel("Год поступления")
plt.ylabel("Общий доход")
plt.tight_layout()
plt.savefig(results_dir / "income_by_year.png")

print("Графики сохранены в папке:", results_dir)

# ==============================
# 5. Генерация SQL-скриптов
# ==============================

from pathlib import Path

# определяю путь до папки проекта и создаю директорию для SQL
base_dir = Path(__file__).resolve().parent
sql_dir = base_dir / "sql"
sql_dir.mkdir(exist_ok=True)

# скрипт для создания таблиц
create_tables_sql = """
-- Создание таблиц для практической работы №3 (Вариант 11)

CREATE TABLE students (
    student_id INTEGER PRIMARY KEY,
    faculty TEXT,
    enrollment_year INTEGER
);

CREATE TABLE scholarships (
    faculty TEXT PRIMARY KEY,
    scholarship_amount INTEGER
);

CREATE TABLE olympiads (
    student_id INTEGER,
    prize_amount INTEGER
);

-- Связи:
-- students.faculty -> scholarships.faculty
-- students.student_id -> olympiads.student_id
"""

# скрипт для объединения данных и расчета дохода
income_query_sql = """
-- Расчет общего дохода студентов (стипендия + призы)

SELECT 
    s.student_id,
    s.faculty,
    s.enrollment_year,
    sch.scholarship_amount,
    o.prize_amount,
    (COALESCE(sch.scholarship_amount,0) + COALESCE(o.prize_amount,0)) AS total_income
FROM students s
LEFT JOIN scholarships sch ON s.faculty = sch.faculty
LEFT JOIN olympiads o ON s.student_id = o.student_id;

-- Средний доход по предметам
SELECT 
    s.faculty,
    AVG(COALESCE(sch.scholarship_amount,0) + COALESCE(o.prize_amount,0)) AS avg_total_income
FROM students s
LEFT JOIN scholarships sch ON s.faculty = sch.faculty
LEFT JOIN olympiads o ON s.student_id = o.student_id
GROUP BY s.faculty
ORDER BY avg_total_income DESC;
"""

# сохраняю оба файла
with open(sql_dir / "create_tables.sql", "w", encoding="utf-8") as f:
    f.write(create_tables_sql)

with open(sql_dir / "income_query.sql", "w", encoding="utf-8") as f:
    f.write(income_query_sql)

print("5. SQL-скрипты успешно созданы в папке:", sql_dir)
print("- create_tables.sql — структура базы данных")
print("- income_query.sql — расчёт и агрегирование доходов")