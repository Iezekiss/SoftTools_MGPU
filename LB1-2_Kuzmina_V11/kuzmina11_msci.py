# -*- coding: utf-8 -*-
"""
Вариант 11: MSCI Indexes — сбор 2–3 страниц таблицы, поля: Index, 1 YR, 5 YR
Выход: CSV/XLSX/PNG + SQLite + SQL-запросы
"""

import re
import time
from typing import Dict, List, Optional

# ---- безопасные импорты матплотлиба (чтобы не падать, если пакет не установлен) ----
try:
    import matplotlib.pyplot as plt
    MATPLOTLIB_OK = True
except Exception:
    MATPLOTLIB_OK = False
    plt = None

import pandas as pd
import sqlite3

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    NoSuchElementException, TimeoutException, StaleElementReferenceException
)

# ========================== НАСТРОЙКИ ==========================
# ВАЖНО: поставьте реальный URL страницы с табличной витриной индексов (с колонками "Index", "1 YR", "5 YR")
START_URL = "https://www.msci.com/our-solutions/indexes"

PAGES_TO_FETCH = 3         # текущая + 2 перехода "Next"
GLOBAL_TIMEOUT = 25        # ожидания Selenium, сек
HEADLESS = True            # запускаем без окна браузера
USE_WEBDRIVER_MANAGER = True  # автоматически скачивать chromedriver

# тексты для cookie-кнопок и пагинации
COOKIE_TEXTS = [
    "Accept all", "Accept All", "I accept", "Allow all", "Accept",
    "Принять", "Согласиться", "Разрешить все"
]
NEXT_TEXTS = [
    "Next", "Next »", "»", "›", "Следующая", "Далее"
]

# ========================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==========================
def percent_to_float(s: Optional[str]) -> Optional[float]:
    if not s:
        return None
    s = s.strip()
    s = re.sub(r"[^\d\-\.,]", "", s).replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None

def make_driver():
    opts = Options()
    if HEADLESS:
        opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1366,800")
    opts.add_argument("--lang=en-US,en")

    if USE_WEBDRIVER_MANAGER:
        from webdriver_manager.chrome import ChromeDriverManager
        service = Service(ChromeDriverManager().install())
        return webdriver.Chrome(service=service, options=opts)
    else:
        # если интернета нет — укажите путь к локальному chromedriver
        service = Service("/path/to/chromedriver")
        return webdriver.Chrome(service=service, options=opts)

def accept_cookies_if_any(driver, wait: WebDriverWait):
    for txt in COOKIE_TEXTS:
        xp = f"//*[self::button or self::a][contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'), '{txt.lower()}')]"
        try:
            el = driver.find_element(By.XPATH, xp)
            el.click()
            time.sleep(0.3)
            return
        except NoSuchElementException:
            continue
        except Exception:
            continue

def locate_table_and_header_map(driver, wait: WebDriverWait):
    """
    Возвращает (table_like_element, colmap) где colmap = {'index': i, '1yr': j, '5yr': k}
    Ищет <table> с <thead><th>... или конструкцию role='grid'
    """
    # Классическая <table>
    tables = driver.find_elements(By.XPATH, "//table")
    for tbl in tables:
        try:
            ths = tbl.find_elements(By.XPATH, ".//thead//th")
            if not ths:
                continue
            headers = [re.sub(r"\s+", " ", th.text).strip().lower() for th in ths]
            def find_idx(names: List[str]) -> Optional[int]:
                for nv in names:
                    for i, h in enumerate(headers):
                        if nv in h:
                            return i
                return None
            idx_i = find_idx(["index", "индекс", "name"])
            y1_i  = find_idx(["1 yr", "1-year", "1y", "1 год"])
            y5_i  = find_idx(["5 yr", "5-year", "5y", "5 лет"])
            if idx_i is not None and y1_i is not None and y5_i is not None:
                return tbl, {"index": idx_i, "1yr": y1_i, "5yr": y5_i}
        except Exception:
            continue

    # ARIA grid
    grids = driver.find_elements(By.XPATH, "//*[@role='grid']")
    for grid in grids:
        try:
            ths = grid.find_elements(By.XPATH, ".//*[@role='columnheader']")
            if not ths:
                continue
            headers = [re.sub(r"\s+", " ", th.text).strip().lower() for th in ths]
            def find_idx(names: List[str]) -> Optional[int]:
                for nv in names:
                    for i, h in enumerate(headers):
                        if nv in h:
                            return i
                return None
            idx_i = find_idx(["index", "индекс", "name"])
            y1_i  = find_idx(["1 yr", "1-year", "1y", "1 год"])
            y5_i  = find_idx(["5 yr", "5-year", "5y", "5 лет"])
            if idx_i is not None and y1_i is not None and y5_i is not None:
                return grid, {"index": idx_i, "1yr": y1_i, "5yr": y5_i}
        except Exception:
            continue

    return None, None

def extract_rows_from_table(tbl, colmap) -> List[Dict[str, str]]:
    rows = []

    # tbody/tr — классика
    body_rows = tbl.find_elements(By.XPATH, ".//tbody/tr")
    if body_rows:
        for tr in body_rows:
            tds = tr.find_elements(By.TAG_NAME, "td")
            if not tds:
                continue
            try:
                name = tds[colmap["index"]].text.strip()
                v1   = tds[colmap["1yr"]].text.strip()
                v5   = tds[colmap["5yr"]].text.strip()
                if name and name.lower() != "index":
                    rows.append({"Index": name, "1 YR": v1, "5 YR": v5})
            except Exception:
                continue
        return rows

    # ARIA grid: role=row / role=gridcell
    grid_rows = tbl.find_elements(By.XPATH, ".//*[@role='row']")
    for gr in grid_rows:
        cells = gr.find_elements(By.XPATH, ".//*[@role='gridcell' or @role='cell']")
        if not cells:
            continue
        try:
            name = cells[colmap["index"]].text.strip()
            v1   = cells[colmap["1yr"]].text.strip()
            v5   = cells[colmap["5yr"]].text.strip()
            if name and name.lower() != "index":
                rows.append({"Index": name, "1 YR": v1, "5 YR": v5})
        except Exception:
            continue
    return rows

def click_next(driver, wait: WebDriverWait) -> bool:
    selectors = [
        "//button[contains(@aria-label,'Next')]",
        "//a[contains(@aria-label,'Next')]",
        "//button[contains(., 'Next') or contains(., 'Следующая') or contains(., 'Далее') or contains(., '›') or contains(., '»')]",
        "//a[contains(., 'Next') or contains(., 'Следующая') or contains(., 'Далее') or contains(., '›') or contains(., '»')]"
    ]
    for xp in selectors:
        try:
            try:
                old_tbody = driver.find_element(By.XPATH, "//table//tbody")
            except NoSuchElementException:
                old_tbody = None
            el = driver.find_element(By.XPATH, xp)
            if not el.is_enabled():
                continue
            driver.execute_script("arguments[0].click();", el)
            if old_tbody:
                WebDriverWait(driver, GLOBAL_TIMEOUT).until(EC.staleness_of(old_tbody))
            else:
                time.sleep(1.2)
            return True
        except NoSuchElementException:
            continue
        except Exception:
            continue
    return False

# ========================== ОСНОВНАЯ ЛОГИКА ==========================
def main():
    if not MATPLOTLIB_OK:
        print("Внимание: пакет matplotlib не найден. Установите: pip install matplotlib")
    driver = make_driver()
    wait = WebDriverWait(driver, GLOBAL_TIMEOUT)

    try:
        print("[1/7] Открываем страницу…")
        driver.get(START_URL)

        print("[2/7] Обрабатываем cookie-баннер (если есть)…")
        time.sleep(1.0)
        accept_cookies_if_any(driver, wait)

        print("[3/7] Ищем таблицу с колонками Index / 1 YR / 5 YR…")
        time.sleep(1.0)
        table, colmap = locate_table_and_header_map(driver, wait)
        if table is None:
            time.sleep(2.0)
            table, colmap = locate_table_and_header_map(driver, wait)
        if table is None:
            raise RuntimeError("Не найдена таблица производительности с нужными колонками.")

        print(f"Найдены колонки: {colmap}")

        all_rows: List[Dict[str, str]] = []
        for page_no in range(1, PAGES_TO_FETCH + 1):
            print(f"[4/7] Сбор строк — страница {page_no}…")
            part = extract_rows_from_table(table, colmap)
            print(f"  → найдено строк: {len(part)}")
            all_rows.extend(part)

            if page_no >= PAGES_TO_FETCH:
                break

            print("[5/7] Переход на следующую страницу…")
            moved = click_next(driver, wait)
            if not moved:
                print("  → пагинация больше недоступна, завершаем.")
                break

            time.sleep(0.8)
            table, colmap = locate_table_and_header_map(driver, wait)
            if table is None:
                print("  → после клика таблица не найдена, завершаем.")
                break

        print(f"[6/7] Всего собрано строк: {len(all_rows)}")
        if not all_rows:
            print("Данные не собраны. Проверьте START_URL и доступность таблицы на странице.")
            return

        # -------- Очистка и анализ ----------
        df = pd.DataFrame(all_rows).drop_duplicates()
        df["1 YR %"] = df["1 YR"].apply(percent_to_float)
        df["5 YR %"] = df["5 YR"].apply(percent_to_float)
        df = df.dropna(subset=["Index", "5 YR %"]).reset_index(drop=True)

        top10 = (df.sort_values("5 YR %", ascending=False)
                   .head(10)
                   .reset_index(drop=True))

        # Вывод в консоль
        print("\nТоп-10 индексов по 5-летней доходности:")
        print(top10[["Index", "1 YR %", "5 YR %"]])

        # -------- Сохранение файлов ----------
        df.to_csv("msci_indexes_1yr_5yr.csv", index=False, encoding="utf-8")
        top10.to_csv("msci_top10_5yr.csv", index=False, encoding="utf-8")
        try:
            top10.to_excel("msci_top10_5yr.xlsx", index=False)
        except Exception:
            # openpyxl может быть не установлен — это не критично
            pass

        # -------- Визуализация ----------
        if MATPLOTLIB_OK and not top10.empty:
            plt.figure(figsize=(10, 6))
            plot = top10.iloc[::-1]
            plt.barh(plot["Index"], plot["5 YR %"])
            for y, v in enumerate(plot["5 YR %"]):
                try:
                    plt.text(v + 0.2, y, f"{v:.2f}%", va="center")
                except Exception:
                    pass
            plt.xlabel("Доходность за 5 лет, %")
            plt.title("MSCI: Топ-10 индексов по 5-летней доходности")
            plt.tight_layout()
            try:
                plt.savefig("msci_top10_5yr.png", dpi=200)
            except Exception:
                pass
            try:
                plt.show()
            except Exception:
                pass
        else:
            print("Визуализация пропущена (нет matplotlib или пустой топ-10).")

        # -------- SQLite + SQL-запросы ----------
        print("\n[7/7] SQLite: запись и запросы…")
        conn = sqlite3.connect("msci_data.db")
        df.to_sql("msci_indexes", conn, if_exists="replace", index=False)
        print("Таблица 'msci_indexes' записана в msci_data.db.")

        # SQL 1: Топ-5 по 5-летней доходности
        sql_top5 = """
        SELECT "Index", "1 YR %", "5 YR %"
        FROM msci_indexes
        WHERE "5 YR %" IS NOT NULL
        ORDER BY "5 YR %" DESC
        LIMIT 5;
        """
        top5_df = pd.read_sql_query(sql_top5, conn)
        print("\nSQL #1 — Топ-5 индексов по 5-летней доходности:")
        print(top5_df)

        # SQL 2: Средняя доходность за 1 и 5 лет
        sql_avg = """
        SELECT AVG("1 YR %") AS avg_1yr, AVG("5 YR %") AS avg_5yr
        FROM msci_indexes
        WHERE "5 YR %" IS NOT NULL;
        """
        avg_df = pd.read_sql_query(sql_avg, conn)
        print("\nSQL #2 — Средняя доходность (1 YR / 5 YR):")
        print(avg_df)

        # SQL 3: Кол-во индексов с 5Y > 10%
        sql_cnt = """
        SELECT COUNT(*) AS cnt_gt_10
        FROM msci_indexes
        WHERE "5 YR %" > 10;
        """
        cnt_df = pd.read_sql_query(sql_cnt, conn)
        print("\nSQL #3 — Число индексов с 5 YR % > 10:")
        print(cnt_df)

        conn.close()

        print("\nГотово. Файлы сохранены:")
        print("  - msci_indexes_1yr_5yr.csv")
        print("  - msci_top10_5yr.csv")
        print("  - msci_top10_5yr.xlsx (если доступен openpyxl)")
        print("  - msci_top10_5yr.png (если доступен matplotlib)")
        print("  - msci_data.db (SQLite, таблица msci_indexes)")

    finally:
        try:
            driver.quit()
        except Exception:
            pass

if __name__ == "__main__":
    main()
