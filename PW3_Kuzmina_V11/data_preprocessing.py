"""
Практическая работа №3 — Вариант 11
Полный цикл обработки данных: генерация, загрузка, предобработка, консолидация и анализ.
Автор: Маргарита Сидоренко
"""

import pandas as pd
import json
import random
from pathlib import Path

# ==============================
# 1. Генерация данных
# ==============================

random.seed(42)

subjects = ["Mathematics", "Physics", "Chemistry", "Social Studies", "Informatics", "History"]
years = [2019, 2020, 2021, 2022, 2023, 2024, 2025]
num_students = 100

base_dir = Path(__file__).resolve().parent
data_dir = base_dir / "data"
data_dir.mkdir(exist_ok=True)

# студенты
students = []
for i in range(1, num_students + 1):
    students.append({
        "student_id": i,
        "faculty": random.choice(subjects),
        "enrollment_year": random.choice(years)
    })

students_df = pd.DataFrame(students)
students_df.to_csv(data_dir / "students.csv", index=False)

# стипендии
scholarships = []
for s in subjects:
    scholarships.append({
        "faculty": s,
        "scholarship_amount": random.choice([1000, 1200, 1400, 1600, 1800])
    })

scholarships_df = pd.DataFrame(scholarships)
scholarships_df.to_excel(data_dir / "scholarships.xlsx", index=False)

# олимпиады
olympiads = []
for s in students:
    olympiads.append({
        "student_id": s["student_id"],
        "prize_amount": random.choice([0, 0, 0, 500, 1000, 1500, 2000])
    })

with open(data_dir / "olympiads.json", "w", encoding="utf-8") as f:
    json.dump(olympiads, f, ensure_ascii=False, indent=4)

print("Данные сгенерированы и сохранены в директории:", data_dir)

# ==============================
# 2. Загрузка и предварительная обработка
# ==============================

students = pd.read_csv(data_dir / "students.csv")
scholarships = pd.read_excel(data_dir / "scholarships.xlsx")
with open(data_dir / "olympiads.json", "r", encoding="utf-8") as f:
    olympiads = pd.DataFrame(json.load(f))

# переименование столбцов в snake_case
students.columns = students.columns.str.lower().str.replace(" ", "_")
scholarships.columns = scholarships.columns.str.lower().str.replace(" ", "_")
olympiads.columns = olympiads.columns.str.lower().str.replace(" ", "_")

# проверка на пропуски и дубликаты
print("\nПроверка пропусков и дубликатов:")
print("students:", students.isnull().sum().sum(), "пропусков,", students.duplicated().sum(), "дубликатов")
print("scholarships:", scholarships.isnull().sum().sum(), "пропусков,", scholarships.duplicated().sum(), "дубликатов")
print("olympiads:", olympiads.isnull().sum().sum(), "пропусков,", olympiads.duplicated().sum(), "дубликатов")

# ==============================
# 3. Консолидация и обогащение
# ==============================

merged = pd.merge(students, scholarships, on="faculty", how="left")
merged = pd.merge(merged, olympiads, on="student_id", how="left")

# обработка пропусков
merged.fillna({"scholarship_amount": 0, "prize_amount": 0}, inplace=True)

# создание нового признака
merged["total_income"] = merged["scholarship_amount"] + merged["prize_amount"]

# ==============================
# 4. Анализ и сохранение результатов
# ==============================

results_dir = base_dir / "results"
results_dir.mkdir(exist_ok=True)

# статистика
print("\nОсновные статистики по общему доходу:")
print(merged["total_income"].describe())

# средний доход по предметам
income_by_faculty = merged.groupby("faculty")["total_income"].mean().reset_index()
income_by_faculty.to_csv(results_dir / "income_summary.csv", index=False)

# сохранение итогового набора
merged.to_csv(results_dir / "merged_dataset.csv", index=False)

print("\nРезультаты сохранены в папку:", results_dir)
print("Файлы:")
print("- income_summary.csv (средний доход по предметам)")
print("- merged_dataset.csv (полный консолидированный набор)")