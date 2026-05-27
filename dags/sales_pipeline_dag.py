"""
DAG: sales_pipeline
Расписание: каждый вторник в 12:45 МСК (09:45 UTC)
"""

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

PG_CONN = {
    "host":     "postgres_user",
    "port":     5432,
    "dbname":   os.getenv("PG_DB",       "sales_db"),
    "user":     os.getenv("PG_USER",     "user"),
    "password": os.getenv("PG_PASSWORD", "user"),
}
CH_HOST  = "clickhouse_user"
CH_PORT  = 9000
DATA_DIR = "/opt/airflow/data"
RAW_CSV  = os.path.join(DATA_DIR, "full_sales_raw.csv")
AGG_CSV  = os.path.join(DATA_DIR, "sales_aggregated.csv")

# ───────────────────────────── Task 1 ─────────────────────────────
def generate_sales(**context):
    import pandas as pd
    import numpy as np
    from datetime import datetime, timedelta

    os.makedirs(DATA_DIR, exist_ok=True)
    rng = np.random.default_rng(seed=42)
    n = 1_000_000

    # Цены с разбросом: дешёвые (1–30), средние (100–500), дорогие (1000–5000)
    product_ids = np.arange(1, 71)
    price_tiers = np.concatenate([
        rng.uniform(1,    30,   size=20),   # товары 1–20: дешёвые
        rng.uniform(100,  500,  size=30),   # товары 21–50: средние
        rng.uniform(1000, 5000, size=20),   # товары 51–70: дорогие
    ])
    price_list = dict(zip(product_ids, np.round(price_tiers, 2)))

    # Каждый регион предпочитает разные товары
    region_product_bias = {
        "North": (1,  30),   # дешёвые товары → маленький средний чек
        "South": (21, 50),   # средние товары
        "East":  (30, 65),   # микс средних и дорогих
        "West":  (51, 70),   # дорогие товары → большой средний чек
    }

    regions_list = []
    product_ids_list = []
    quantity_list = []
    region_weights = {"North": 0.45, "South": 0.25, "East": 0.20, "West": 0.10}

    for region, weight in region_weights.items():
        size = int(n * weight)
        lo, hi = region_product_bias[region]
        regions_list.append(np.full(size, region))
        product_ids_list.append(rng.integers(lo, hi + 1, size=size))

        # Количество тоже разное по регионам
        if region == "North":
            qty = np.clip(rng.integers(1, 3, size=size), 1, 10)    # мало покупают
        elif region == "West":
            qty = np.clip(rng.integers(5, 20, size=size), 1, 50)   # много покупают
        else:
            qty = np.clip(rng.integers(1, 10, size=size), 1, 30)
        quantity_list.append(qty)

    regions_arr    = np.concatenate(regions_list)
    products_arr   = np.concatenate(product_ids_list)
    quantities_arr = np.concatenate(quantity_list)

    # Перемешаем чтобы не шло блоками
    idx = rng.permutation(len(regions_arr))

    # Создаём нужные даты, чтоб был актуальный год
    yesterday  = pd.Timestamp(datetime.now().date() - timedelta(days=1))
    year_ago   = pd.Timestamp(datetime.now().date() - timedelta(days=365))

    df = pd.DataFrame({
        "sale_id":     range(1, len(idx) + 1),
        "customer_id": rng.integers(1, 5_001, size=len(idx)),
        "product_id":  products_arr[idx],
        "quantity":    quantities_arr[idx],
        "sale_date":   pd.to_datetime(
            rng.integers(
                int(year_ago.timestamp()),
                int(yesterday.timestamp()),
                size=len(idx)
            ), unit="s"
        ).normalize(),
        "region": regions_arr[idx],
    })

    df["price_product"] = df["product_id"].map(price_list)
    df["sale_amount"]   = (df["quantity"] * df["price_product"]).round(2)

    df[[
        "sale_id", "customer_id", "product_id", "quantity",
        "sale_date", "price_product", "sale_amount", "region"
    ]].to_csv(RAW_CSV, index=False)

# ───────────────────────────── Task 2 ─────────────────────────────
def spark_process(**context):
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import LongType
    from pyspark.sql import Window


    spark = (
        SparkSession.builder
        .appName("SalesPipeline")
        .master("spark://spark-master:7077")
        .getOrCreate()
    )

    df = spark.read.csv(RAW_CSV, header=True, inferSchema=True)
    df = df.dropDuplicates()

    window_region = Window.partitionBy('region')
    window_month_year = Window.partitionBy('sale_month', 'sale_year')
    window_product_id = Window.partitionBy('product_id')

    agg_df = df.groupBy('region', 'product_id', F.date_format('sale_date', 'MMMM').alias('sale_month'), F.month('sale_date').alias('num_month'), F.year('sale_date').alias('sale_year')) \
                .agg(
                    F.round(F.avg('sale_amount'), 1).alias('avg_receipt'),
                    F.round(F.sum('sale_amount'), 1).cast(LongType()).alias('sum_amount'),
                    F.count('sale_date').alias('count_sales'),
                    F.sum('quantity').alias('sum_quantity'),
                    F.round(F.avg('quantity'), 1).alias('avg_receipt_quantity')) \
                .withColumns({
                    'avg_receipt_region': F.round(F.avg('avg_receipt')    .over(window_region), 1),
                    'sum_amount_region': F.sum('sum_amount')              .over(window_region),
                    'count_sales_region': F.sum('count_sales')            .over(window_region),
                    'sum_quantity_region': F.sum('sum_quantity')          .over(window_region),
                    'avg_receipt_quantity_region': F.round(F.avg('avg_receipt_quantity').over(window_region), 1),

                    'avg_receipt_month_year': F.round(F.avg('avg_receipt')    .over(window_month_year), 1),
                    'sum_amount_month_year': F.sum('sum_amount')              .over(window_month_year),
                    'count_sales_month_year': F.sum('count_sales')            .over(window_month_year),
                    'sum_quantity_month_year': F.sum('sum_quantity')          .over(window_month_year),
                    'avg_receipt_quantity_month_year': F.round(F.avg('avg_receipt_quantity').over(window_month_year), 1),

                    'avg_receipt_product_id': F.round(F.avg('avg_receipt')    .over(window_product_id), 1),
                    'sum_amount_product_id': F.sum('sum_amount')              .over(window_product_id),
                    'count_sales_product_id': F.sum('count_sales')            .over(window_product_id),
                    'sum_quantity_product_id': F.sum('sum_quantity')          .over(window_product_id),
                    'avg_receipt_quantity_product_id': F.round(F.avg('avg_receipt_quantity').over(window_product_id), 1),
                })

    agg_df.toPandas().to_csv(AGG_CSV, index=False)
    spark.stop()

# ───────────────────────────── Task 3 ─────────────────────────────
def load_postgres(**context):
    import psycopg2
    import pandas as pd
    from psycopg2.extras import execute_values

    conn = psycopg2.connect(**PG_CONN)
    cur  = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS sales (
            sale_id       BIGINT PRIMARY KEY,
            customer_id   INT,
            product_id    INT,
            quantity      INT,
            sale_date     DATE,
            price_product NUMERIC(10,2),
            sale_amount   NUMERIC(12,2),
            region        VARCHAR(10)
        );
        TRUNCATE TABLE sales;
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS sales_aggregated (
            region                           VARCHAR(10),
            product_id                       INT,
            sale_month                       VARCHAR(20),
            num_month                        INT,
            sale_year                        INT,
            avg_receipt                      NUMERIC(12,1),
            sum_amount                       BIGINT,
            count_sales                      BIGINT,
            sum_quantity                     BIGINT,
            avg_receipt_quantity             NUMERIC(6,1),
            avg_receipt_region               NUMERIC(12,1),
            sum_amount_region                BIGINT,
            count_sales_region               BIGINT,
            sum_quantity_region              BIGINT,
            avg_receipt_quantity_region      NUMERIC(6,1),
            avg_receipt_month_year           NUMERIC(12,1),
            sum_amount_month_year            BIGINT,
            count_sales_month_year           BIGINT,
            sum_quantity_month_year          BIGINT,
            avg_receipt_quantity_month_year  NUMERIC(6,1),
            avg_receipt_product_id           NUMERIC(12,1),
            sum_amount_product_id            BIGINT,
            count_sales_product_id           BIGINT,
            sum_quantity_product_id          BIGINT,
            avg_receipt_quantity_product_id  NUMERIC(6,1)
        );
        ALTER TABLE sales_aggregated
            ADD COLUMN IF NOT EXISTS avg_receipt                      NUMERIC(12,1),
            ADD COLUMN IF NOT EXISTS sum_amount                       BIGINT,
            ADD COLUMN IF NOT EXISTS count_sales                      BIGINT,
            ADD COLUMN IF NOT EXISTS sum_quantity                     BIGINT,
            ADD COLUMN IF NOT EXISTS avg_receipt_quantity             NUMERIC(6,1),
            ADD COLUMN IF NOT EXISTS avg_receipt_region               NUMERIC(12,1),
            ADD COLUMN IF NOT EXISTS sum_amount_region                BIGINT,
            ADD COLUMN IF NOT EXISTS count_sales_region               BIGINT,
            ADD COLUMN IF NOT EXISTS sum_quantity_region              BIGINT,
            ADD COLUMN IF NOT EXISTS avg_receipt_quantity_region      NUMERIC(6,1),
            ADD COLUMN IF NOT EXISTS avg_receipt_month_year           NUMERIC(12,1),
            ADD COLUMN IF NOT EXISTS sum_amount_month_year            BIGINT,
            ADD COLUMN IF NOT EXISTS count_sales_month_year           BIGINT,
            ADD COLUMN IF NOT EXISTS sum_quantity_month_year          BIGINT,
            ADD COLUMN IF NOT EXISTS avg_receipt_quantity_month_year  NUMERIC(6,1),
            ADD COLUMN IF NOT EXISTS avg_receipt_product_id           NUMERIC(12,1),
            ADD COLUMN IF NOT EXISTS sum_amount_product_id            BIGINT,
            ADD COLUMN IF NOT EXISTS count_sales_product_id           BIGINT,
            ADD COLUMN IF NOT EXISTS sum_quantity_product_id          BIGINT,
            ADD COLUMN IF NOT EXISTS avg_receipt_quantity_product_id  NUMERIC(6,1);
        TRUNCATE TABLE sales_aggregated;
    """)
    conn.commit()

    df_raw = pd.read_csv(RAW_CSV)
    cols_raw = ["sale_id", "customer_id", "product_id", "quantity",
                "sale_date", "price_product", "sale_amount", "region"]
    for i in range(0, len(df_raw), 50_000):
        chunk = df_raw[cols_raw].iloc[i:i + 50_000]
        execute_values(cur, """
            INSERT INTO sales
              (sale_id, customer_id, product_id, quantity,
               sale_date, price_product, sale_amount, region)
            VALUES %s
        """, [tuple(r) for r in chunk.itertuples(index=False)])
        conn.commit()

    df_agg = pd.read_csv(AGG_CSV)
    cols_agg = [
        "region", "product_id", "sale_month", "num_month", "sale_year",
        "avg_receipt", "sum_amount", "count_sales", "sum_quantity", "avg_receipt_quantity",
        "avg_receipt_region", "sum_amount_region", "count_sales_region",
        "sum_quantity_region", "avg_receipt_quantity_region",
        "avg_receipt_month_year", "sum_amount_month_year", "count_sales_month_year",
        "sum_quantity_month_year", "avg_receipt_quantity_month_year",
        "avg_receipt_product_id", "sum_amount_product_id", "count_sales_product_id",
        "sum_quantity_product_id", "avg_receipt_quantity_product_id",
    ]
    execute_values(cur, """
        INSERT INTO sales_aggregated
          (region, product_id, sale_month, num_month, sale_year,
           avg_receipt, sum_amount, count_sales, sum_quantity, avg_receipt_quantity,
           avg_receipt_region, sum_amount_region, count_sales_region,
           sum_quantity_region, avg_receipt_quantity_region,
           avg_receipt_month_year, sum_amount_month_year, count_sales_month_year,
           sum_quantity_month_year, avg_receipt_quantity_month_year,
           avg_receipt_product_id, sum_amount_product_id, count_sales_product_id,
           sum_quantity_product_id, avg_receipt_quantity_product_id)
        VALUES %s
    """, [tuple(r) for r in df_agg[cols_agg].itertuples(index=False)])
    conn.commit()

    cur.close()
    conn.close()


# ───────────────────────────── Task 4 ─────────────────────────────
def load_clickhouse(**context):
    from clickhouse_driver import Client
    import pandas as pd

    client = Client(host=CH_HOST, port=CH_PORT)

    client.execute("""
        CREATE TABLE IF NOT EXISTS sales_aggregated (
            region                           String,
            product_id                       Int32,
            sale_month                       String,
            num_month                        Int32,
            sale_year                        Int32,
            avg_receipt                      Float64,
            sum_amount                       Int64,
            count_sales                      Int64,
            sum_quantity                     Int64,
            avg_receipt_quantity             Float64,
            avg_receipt_region               Float64,
            sum_amount_region                Int64,
            count_sales_region               Int64,
            sum_quantity_region              Int64,
            avg_receipt_quantity_region      Float64,
            avg_receipt_month_year           Float64,
            sum_amount_month_year            Int64,
            count_sales_month_year           Int64,
            sum_quantity_month_year          Int64,
            avg_receipt_quantity_month_year  Float64,
            avg_receipt_product_id           Float64,
            sum_amount_product_id            Int64,
            count_sales_product_id           Int64,
            sum_quantity_product_id          Int64,
            avg_receipt_quantity_product_id  Float64,
            import_date                      Date
        ) ENGINE = MergeTree()
        ORDER BY (sale_year, num_month)
    """)
    client.execute("TRUNCATE TABLE IF EXISTS sales_aggregated")

    df_agg = pd.read_csv(AGG_CSV)
    df_agg["import_date"] = datetime.now().date()

    cols = [
        "region", "product_id", "sale_month", "num_month", "sale_year",
        "avg_receipt", "sum_amount", "count_sales", "sum_quantity", "avg_receipt_quantity",
        "avg_receipt_region", "sum_amount_region", "count_sales_region",
        "sum_quantity_region", "avg_receipt_quantity_region",
        "avg_receipt_month_year", "sum_amount_month_year", "count_sales_month_year",
        "sum_quantity_month_year", "avg_receipt_quantity_month_year",
        "avg_receipt_product_id", "sum_amount_product_id", "count_sales_product_id",
        "sum_quantity_product_id", "avg_receipt_quantity_product_id",
        "import_date",
    ]

    client.execute(
        "INSERT INTO sales_aggregated VALUES",
        [list(r) for r in df_agg[cols].itertuples(index=False)]
    )

# ───────────────────────────── Task 5 ─────────────────────────────
def notify_success(**context):
    import os
    import requests
    import psycopg2
    from datetime import datetime
    from clickhouse_driver import Client

    token   = os.getenv("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "")

    # ── ClickHouse: список таблиц + строки ───────────────────────
    ch_client = Client(host=CH_HOST, port=CH_PORT)

    ch_tables = ch_client.execute("""
        SELECT
            name,
            total_rows
        FROM system.tables
        WHERE database = currentDatabase()
          AND engine NOT IN ('View', 'MaterializedView', 'Dictionary')
        ORDER BY name
    """)
    # ch_tables = [(table_name, row_count), ...]

    ch_total = sum(cnt for _, cnt in ch_tables)

    # Время последнего импорта — из твоей агрегированной таблицы
    ts_result = ch_client.execute(
        "SELECT max(import_date) FROM sales_aggregated"
    )
    import_dt = ts_result[0][0]  # datetime или date

    # Форматируем: дата + время
    now_dt = datetime.now()
    import_dt_str = now_dt.strftime("%Y-%m-%d %H:%M:%S")

    ch_lines = "\n".join(
        f"  • `{name}`: `{(cnt or 0):,}`" for name, cnt in ch_tables
    )

    # ── PostgreSQL: список таблиц + точный COUNT ─────────────────
    pg_conn = psycopg2.connect(
        host=os.getenv("PG_HOST", "postgres_user"),
        port=int(os.getenv("PG_PORT", 5432)),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        dbname=os.getenv("PG_DB"),
    )
    try:
        with pg_conn.cursor() as cur:
            cur.execute("""
                SELECT tablename FROM pg_tables
                WHERE schemaname = 'public'
                ORDER BY tablename
            """)
            tables = [row[0] for row in cur.fetchall()]

            pg_tables = []
            for table in tables:
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                pg_tables.append((table, cur.fetchone()[0]))
    finally:
        pg_conn.close()

    pg_total = sum(cnt for _, cnt in pg_tables)
    pg_lines = "\n".join(
        f"  • `{name}`: `{cnt:,}`" for name, cnt in pg_tables
    )

    # ── Telegram ──────────────────────────────────────────────────
    msg = (
        f"✅ *Sales Pipeline завершён*\n\n"
        f"📅 Дата и время импорта: `{import_dt_str}`\n\n"
        f"🐘 *PostgreSQL* (итого: `{pg_total:,}`):\n"
        f"{pg_lines}\n"
        f"🟡 *ClickHouse* (итого: `{ch_total:,}`):\n"
        f"{ch_lines}\n"
    )

    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": msg, "parse_mode": "Markdown"},
            timeout=15,
        )
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"[notify_success] Telegram send failed: {e}")


def notify_failure(context):
    import requests

    token   = os.getenv("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "")

    task_id   = context.get("task_instance").task_id
    dag_id    = context.get("task_instance").dag_id
    exec_date = context.get("execution_date")
    exception = context.get("exception")

    msg = (
        f"❌ *Ошибка в DAG*\n\n"
        f"DAG: `{dag_id}`\n"
        f"Таска: `{task_id}`\n"
        f"Время: `{exec_date}`\n"
        f"Ошибка: `{exception}`"
    )
    requests.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        json={"chat_id": chat_id, "text": msg, "parse_mode": "Markdown"}
    )

# ─────────────────────────── DAG definition ───────────────────────
default_args = {
    "owner":               "airflow",
    "retries":             1,
    "retry_delay":         timedelta(minutes=5),
    "on_failure_callback": notify_failure,
}

with DAG(
    dag_id="sales_pipeline",
    default_args=default_args,
    description="Sales ETL: generate -> spark -> postgres -> clickhouse",
    schedule_interval="45 9 * * 2",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["sales", "etl"],
) as dag:

    t1 = PythonOperator(task_id="generate_sales",  python_callable=generate_sales)
    t2 = PythonOperator(task_id="spark_process",   python_callable=spark_process)
    t3 = PythonOperator(task_id="load_postgres",   python_callable=load_postgres)
    t4 = PythonOperator(task_id="load_clickhouse", python_callable=load_clickhouse)
    t5 = PythonOperator(task_id="notify_telegram", python_callable=notify_success)

    t1 >> t2 >> t3 >> t4 >> t5
