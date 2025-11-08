
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
