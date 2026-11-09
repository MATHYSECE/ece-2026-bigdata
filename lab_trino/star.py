import os

import pandas as pd
import trino

pd.set_option("display.width", 200)
pd.set_option("display.max_columns", 20)

conn = trino.dbapi.connect(
    host=os.environ["TRINO_HOST"],
    port=int(os.environ["TRINO_PORT"]),
    user=os.environ["TRINO_USER"],
    http_scheme="http",
    catalog="lakehouse",
)


def sql(statement):
    cur = conn.cursor()
    cur.execute(statement)
    rows = cur.fetchall()
    columns = [c[0] for c in cur.description] if cur.description else []
    return pd.DataFrame(rows, columns=columns)


def show(title, statement):
    print(f"\n== {title}")
    print(sql(statement).to_string())



sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.gold.dim_user AS
    SELECT row_number() OVER (ORDER BY user_id) AS user_key,
           user_id, username, name, sex, birthdate, city, state, zip_code
    FROM lakehouse.silver.users
""")
sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.gold.dim_product AS
    SELECT row_number() OVER (ORDER BY product) AS product_key, product
    FROM (SELECT DISTINCT product FROM lakehouse.silver.orders)
""")

first_day, last_day = sql("""
    SELECT CAST(min(ordered_at) AS date) AS first_day,
           CAST(max(ordered_at) AS date) AS last_day
    FROM lakehouse.silver.orders
""").iloc[0]
print("date range:", first_day, "->", last_day)

sql(f"""
    CREATE TABLE IF NOT EXISTS lakehouse.gold.dim_date AS
    SELECT CAST(date_format(CAST(d AS timestamp), '%Y%m%d') AS integer) AS date_key,
           d AS full_date, year(d) AS year, quarter(d) AS quarter, month(d) AS month,
           date_format(CAST(d AS timestamp), '%M') AS month_name,
           day_of_week(d) AS day_of_week, day_of_week(d) IN (6, 7) AS is_weekend
    FROM UNNEST(sequence(DATE '{first_day}', DATE '{last_day}', INTERVAL '1' DAY)) AS t(d)
""")

sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.gold.fact_orders AS
    SELECT o.order_id, u.user_key, p.product_key,
           CAST(date_format(o.ordered_at, '%Y%m%d') AS integer) AS date_key,
           o.ordered_at, o.quantity
    FROM lakehouse.silver.orders AS o
    LEFT JOIN lakehouse.gold.dim_user AS u ON u.user_id = o.user_id
    LEFT JOIN lakehouse.gold.dim_product AS p ON p.product = o.product
""")

show("row counts", """
    SELECT 'dim_user' AS table_name, count(*) AS row_count FROM lakehouse.gold.dim_user
    UNION ALL SELECT 'dim_product', count(*) FROM lakehouse.gold.dim_product
    UNION ALL SELECT 'dim_date', count(*) FROM lakehouse.gold.dim_date
    UNION ALL SELECT 'fact_orders', count(*) FROM lakehouse.gold.fact_orders
""")
show("dim_product", "SELECT * FROM lakehouse.gold.dim_product ORDER BY product_key")
show("dim_date sample", "SELECT * FROM lakehouse.gold.dim_date ORDER BY date_key LIMIT 5")
show("fact_orders sample", "SELECT * FROM lakehouse.gold.fact_orders LIMIT 5")

show("top 10 (year, state, product)", """
    SELECT d.year, u.state, p.product, count(*) AS orders, sum(f.quantity) AS units
    FROM lakehouse.gold.fact_orders AS f
    JOIN lakehouse.gold.dim_date AS d ON d.date_key = f.date_key
    JOIN lakehouse.gold.dim_user AS u ON u.user_key = f.user_key
    JOIN lakehouse.gold.dim_product AS p ON p.product_key = f.product_key
    GROUP BY d.year, u.state, p.product
    ORDER BY units DESC
    LIMIT 10
""")

STAR_BY_STATE_MONTH = """
    SELECT u.state, d.year, d.month, count(*) AS orders,
           sum(f.quantity) AS units, count(DISTINCT f.user_key) AS buyers
    FROM lakehouse.gold.fact_orders AS f
    JOIN lakehouse.gold.dim_date AS d ON d.date_key = f.date_key
    JOIN lakehouse.gold.dim_user AS u ON u.user_key = f.user_key
    GROUP BY u.state, d.year, d.month
"""
show("star: orders by state and month", STAR_BY_STATE_MONTH + " ORDER BY d.year, d.month, u.state LIMIT 10")

print("\n== validation")
silver_orders = sql("SELECT count(*) AS v FROM lakehouse.silver.orders").iloc[0, 0]
fact_orders = sql("SELECT count(*) AS v FROM lakehouse.gold.fact_orders").iloc[0, 0]
silver_units = sql("SELECT sum(quantity) AS v FROM lakehouse.silver.orders").iloc[0, 0]
fact_units = sql("SELECT sum(quantity) AS v FROM lakehouse.gold.fact_orders").iloc[0, 0]
assert silver_orders == fact_orders, f"{silver_orders} != {fact_orders}"
assert silver_units == fact_units, f"{silver_units} != {fact_units}"
unmatched = sql("""
    SELECT count(*) AS v
    FROM lakehouse.gold.fact_orders AS f
    LEFT JOIN lakehouse.gold.dim_user AS u ON u.user_key = f.user_key
    LEFT JOIN lakehouse.gold.dim_product AS p ON p.product_key = f.product_key
    LEFT JOIN lakehouse.gold.dim_date AS d ON d.date_key = f.date_key
    WHERE u.user_key IS NULL OR p.product_key IS NULL OR d.date_key IS NULL
""").iloc[0, 0]
assert unmatched == 0, f"{unmatched} orders without a complete dimension row"

mismatch = sql(f"""
    WITH star AS ({STAR_BY_STATE_MONTH}),
    gold AS (
        SELECT state, year(month) AS year, month(month) AS month, orders, units, buyers
        FROM lakehouse.gold.orders_by_state_month
    )
    SELECT count(*) AS v
    FROM star s FULL OUTER JOIN gold g
      ON s.state = g.state AND s.year = g.year AND s.month = g.month
     AND s.orders = g.orders AND s.units = g.units AND s.buyers = g.buyers
    WHERE s.state IS NULL OR g.state IS NULL
""").iloc[0, 0]
assert mismatch == 0, f"{mismatch} rows differ between the star and orders_by_state_month"
print("validation OK:", silver_orders, fact_orders, silver_units, fact_units, unmatched, mismatch)

conn.close()
