
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
