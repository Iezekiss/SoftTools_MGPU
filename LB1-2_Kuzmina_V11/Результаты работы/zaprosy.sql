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

-- SQL #1 — Топ-5 индексов по 5-летней доходности:
--                        Index  1 YR %  5 YR %
-- 0  MSCI Czech Republic Index    76.0    35.9
-- 1       MSCI Argentina Index   -10.9    33.2
-- 2        MSCI Bulgaria Index    63.6    33.0
-- 3          MSCI Greece Index    74.8    30.9
-- 4         MSCI Austria Index    58.5    26.9

-- SQL #2 — Средняя доходность (1 YR / 5 YR):
--    avg_1yr  avg_5yr
-- 0   22.015    12.58

-- SQL #3 — Число индексов с 5 YR % > 10:
--    cnt_gt_10
-- 0         37