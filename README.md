# 🧩 SoftTools_MGPU  
Лабораторные и практические работы Кузьминой Д. Ю., студентки группы **БД-241м**,  
по дисциплине **«Программные средства сбора, консолидации и аналитики данных»**

---

## 🎯 Цель курса  
Подготовить специалистов к полному циклу работы с данными —  
от автоматического сбора информации из различных веб-источников и консолидации,  
до анализа и визуализации данных для принятия решений.

### Основные задачи курса:
- автоматизация сбора данных через API и веб-парсинг;  
- консолидация и очистка данных для последующего анализа;  
- применение инструментов статистического и интеллектуального анализа;  
- визуализация результатов и подготовка аналитических отчётов;  
- хранение данных в базах SQLite и интеграция с Python-средой.

---

## 🧠 Используемые технологии
**Python**, **Pandas**, **Matplotlib**, **Seaborn**, **Requests**, **BeautifulSoup**,  
**Selenium WebDriver**, **SQLite**, **Jupyter / Google Colab**, **Git**.

---

## 📂 Текущая структура репозитория

```

├── LB1-2_Kuzmina_V11
│   ├── kuzmina11_msci.py
│   ├── msci_top10_5yr.png
│   ├── ОТЧЕТ ЛБ1-2.docx
│   ├── ОТЧЕТ ЛБ1-2.pdf
│   └── Результаты работы
│       ├── msci_data.db
│       ├── msci_indexes_1yr_5yr.csv
│       ├── msci_top10_5yr (1).png
│       ├── msci_top10_5yr.csv
│       ├── msci_top10_5yr.xlsx
│       └── zaprosy.sql
├── LB3_Kuzmina_V11 
│   ├── cleanup.sh
│   ├── dags
│   │   ├── dagKu11.py
│   │   ├── data
│   │   ├── kuzmina11
│   │   │   ├── data
│   │   │   │   ├── categories.xlsx
│   │   │   │   ├── sales.json
│   │   │   │   └── stores.csv
│   │   │   └── generate_d.py
│   │   └── test.py
│   ├── docker-compose.yml
│   ├── lab3_variant11_project
│   │   ├── data
│   │   │   ├── categories.xlsx
│   │   │   ├── sales.json
│   │   │   └── stores.csv
│   │   ├── output
│   │   │   └── top_categories.db
│   │   └── reports
│   │       ├── plot_revenue_by_city.png
│   │       ├── plot_revenue_share.png
│   │       ├── plot_top3_categories.png
│   │       └── report_lab3_variant11.txt
│   ├── README.md
│   ├── requirements.txt
│   ├── Архитектура
│   │   ├── Снимок экрана 2025-11-08 в 22.58.56.png
│   │   ├── Снимок экрана 2025-11-08 в 22.59.03.png
│   │   └── Снимок экрана 2025-11-08 в 22.59.17.png
│   ├── ОТЧЕТ ЛБ3.docx
│   └── ОТЧЕТ ЛБ3.pdf
├── PW1_Kuzmina_V11
│   ├── PW_1_API_Analysis_Kuzmina.ipynb
│   ├── ОТЧЕТ ПР1.docx
│   └── ОТЧЕТ ПР1.pdf
├── PW2_Kuzmina_V11
│   ├── PW_2_2025_Kuzmina.ipynb
│   ├── ОТЧЕТ ПР2.docx
│   └── ОТЧЕТ ПР2.pdf
├── PW3_Kuzmina_V11
│   ├── data
│   │   ├── olympiads.json
│   │   ├── scholarships.xlsx
│   │   └── students.csv
│   ├── data_generator.py
│   ├── data_preprocessing.py
│   ├── docker_help.md
│   ├── docker-compose.yml
│   ├── Dockerfile
│   ├── main_analysis.py
│   ├── README.md
│   ├── results
│   │   ├── income_by_faculty.png
│   │   ├── income_by_year.png
│   │   ├── income_distribution.png
│   │   ├── income_summary.csv
│   │   └── merged_dataset.csv
│   ├── sql
│   │   ├── create_tables.sql
│   │   └── income_query.sql
│   ├── Архитектура аналитического решения — копия.drawio
│   ├── Архитектура аналитического решения — копия.png
│   ├── ОТЧЕТ ПР3.docx
│   └── ОТЧЕТ ПР3.pdf
└── README.md

## 📊 Содержание курса

| Раздел                  | Работы          | Баллы |
| :---------------------- | :-------------- | :---: |
| **Сбор данных**         | ПР 1, 2, ЛБ 1–2 |   30  |
| **Консолидация данных** | ПР 3, ЛБ 3      |   15  |
| **Аналитика данных**    | ПР-ЛБ 4         |   10  |
| **Итоговый зачёт**      | Тестирование    | зачёт |

---

## 🧾 Автор

**Кузьмина Дарья Юрьевна**
Магистрант группы **БД-241м**
**Московский городской педагогический университет**, 2025 г.
