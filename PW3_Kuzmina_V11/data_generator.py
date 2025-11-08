import pandas as pd
import json
import random
from pathlib import Path

# фиксирую seed чтобы при каждом запуске получались одинаковые данные
random.seed(42)

# основные настройки
subjects = ["Mathematics", "Physics", "Chemistry", "Social Studies", "Informatics", "History"]
years = [2019, 2020, 2021, 2022, 2023, 2024, 2025]
num_students = 100

# создаю абсолютный путь к директории со скриптом
base_dir = Path(__file__).resolve().parent
data_dir = base_dir / "data"
data_dir.mkdir(exist_ok=True)

# генерирую студентов
students = []
for i in range(1, num_students + 1):
    students.append({
        "student_id": i,
        "faculty": random.choice(subjects),
        "enrollment_year": random.choice(years)
    })

students_df = pd.DataFrame(students)
students_df.to_csv(data_dir / "students.csv", index=False)

# генерирую стипендии
scholarships = []
for s in subjects:
    scholarships.append({
        "faculty": s,
        "scholarship_amount": random.choice([1000, 1200, 1400, 1600, 1800])
    })

scholarships_df = pd.DataFrame(scholarships)
scholarships_df.to_excel(data_dir / "scholarships.xlsx", index=False)

# генерирую призовые
olympiads = []
for s in students:
    olympiads.append({
        "student_id": s["student_id"],
        "prize_amount": random.choice([0, 0, 0, 500, 1000, 1500, 2000])
    })

with open(data_dir / "olympiads.json", "w", encoding="utf-8") as f:
    json.dump(olympiads, f, ensure_ascii=False, indent=4)

# проверяю результат
print("Папка данных:", data_dir)
print("Файлы сгенерированы:")
for file in data_dir.iterdir():
    print("-", file.name)

print("\nПервые 5 студентов:")
print(students_df.head())

prizes = [o["prize_amount"] for o in olympiads]
print("Студентов с призами:", sum(p > 0 for p in prizes))
print("Средний размер приза:", sum(prizes) / len(prizes))