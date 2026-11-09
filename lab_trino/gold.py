import os

import pandas as pd
import trino

pd.set_option("display.width", 200)
pd.set_option("display.max_columns", 20)
pd.set_option("display.max_colwidth", 60)

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


show("version", "SELECT version()")
show("catalogs", "SHOW CATALOGS")
show("schemas", "SHOW SCHEMAS FROM lakehouse")
show("tables in silver", "SHOW TABLES FROM lakehouse.silver")
show("describe orders", "DESCRIBE lakehouse.silver.orders")
show("describe users", "DESCRIBE lakehouse.silver.users")
show("count orders", "SELECT count(*) AS orders FROM lakehouse.silver.orders")
show("count users", "SELECT count(*) AS users FROM lakehouse.silver.users")
show("users by state", """
    SELECT state, count(*) AS users, count_if(city IS NULL) AS missing_city
    FROM lakehouse.silver.users GROUP BY state ORDER BY users DESC LIMIT 10
""")
show("orphan orders", """
    SELECT count(*) AS orphan_orders
    FROM lakehouse.silver.orders AS o
    LEFT JOIN lakehouse.silver.users AS u ON u.user_id = o.user_id
    WHERE u.user_id IS NULL
""")
show("orders snapshots", 'SELECT snapshot_id, committed_at, operation FROM lakehouse.silver."orders$snapshots" ORDER BY committed_at')
show("orders files", 'SELECT file_path, file_format, record_count, file_size_in_bytes FROM lakehouse.silver."orders$files"')
show("orders history", 'SELECT * FROM lakehouse.silver."orders$history"')

JOIN_QUERY = """
    SELECT u.state, count(*) AS orders
    FROM lakehouse.silver.orders AS o
    JOIN lakehouse.silver.users AS u ON u.user_id = o.user_id
    GROUP BY u.state
"""
print("\n== distributed plan")
print(sql("EXPLAIN (TYPE DISTRIBUTED) " + JOIN_QUERY).iloc[0, 0])
print("\n== explain analyze")
print(sql("EXPLAIN ANALYZE " + JOIN_QUERY).iloc[0, 0])
try:
    show("recent queries", """
        SELECT query_id, state, date_diff('millisecond', created, coalesce("end", current_timestamp)) AS elapsed_ms, query
        FROM system.runtime.queries ORDER BY created DESC LIMIT 5
    """)
except Exception as e:
    print("system.runtime.queries denied:", str(e)[:200])

print("\n== gold layer")
sql("CREATE SCHEMA IF NOT EXISTS lakehouse.gold")
show("schemas after create", "SHOW SCHEMAS FROM lakehouse")

sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.gold.orders_by_state_month AS
    SELECT u.state, date_trunc('month', o.ordered_at) AS month,
           count(*) AS orders, sum(o.quantity) AS units, count(DISTINCT o.user_id) AS buyers
    FROM lakehouse.silver.orders AS o
    JOIN lakehouse.silver.users AS u ON u.user_id = o.user_id
    GROUP BY 1, 2
""")
show("orders_by_state_month", "SELECT * FROM lakehouse.gold.orders_by_state_month ORDER BY month, state LIMIT 10")

sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.gold.top_products AS
    SELECT product, count(*) AS orders, sum(quantity) AS units,
           rank() OVER (ORDER BY sum(quantity) DESC) AS units_rank
    FROM lakehouse.silver.orders GROUP BY product
""")
show("top_products", "SELECT * FROM lakehouse.gold.top_products ORDER BY units_rank")

sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.gold.user_activity AS
    SELECT u.user_id, u.state, date_diff('year', u.birthdate, current_date) AS age,
           count(o.order_id) AS orders, coalesce(sum(o.quantity), 0) AS units,
           min(o.ordered_at) AS first_order_at, max(o.ordered_at) AS last_order_at
    FROM lakehouse.silver.users AS u
    LEFT JOIN lakehouse.silver.orders AS o ON o.user_id = u.user_id
    GROUP BY u.user_id, u.state, u.birthdate
""")
show("user_activity", "SELECT * FROM lakehouse.gold.user_activity ORDER BY orders DESC LIMIT 10")
show("gold tables", "SHOW TABLES FROM lakehouse.gold")
show("top_products files", 'SELECT file_path, record_count, file_size_in_bytes FROM lakehouse.gold."top_products$files"')

print("\n== validation")
silver_units = sql("SELECT sum(quantity) AS v FROM lakehouse.silver.orders").iloc[0, 0]
gold_units = sql("SELECT sum(units) AS v FROM lakehouse.gold.orders_by_state_month").iloc[0, 0]
product_units = sql("SELECT sum(units) AS v FROM lakehouse.gold.top_products").iloc[0, 0]
assert silver_units == gold_units
assert silver_units == product_units
silver_users = sql("SELECT count(*) AS v FROM lakehouse.silver.users").iloc[0, 0]
gold_users = sql("SELECT count(*) AS v FROM lakehouse.gold.user_activity").iloc[0, 0]
silver_orders = sql("SELECT count(*) AS v FROM lakehouse.silver.orders").iloc[0, 0]
gold_orders = sql("SELECT sum(orders) AS v FROM lakehouse.gold.user_activity").iloc[0, 0]
assert silver_users == gold_users
assert silver_orders == gold_orders
print("validation OK:", silver_units, gold_units, product_units, silver_users, gold_users, silver_orders, gold_orders)

print("\n== refresh top_products")
sql("DELETE FROM lakehouse.gold.top_products")
sql("""
    INSERT INTO lakehouse.gold.top_products
    SELECT product, count(*), sum(quantity), rank() OVER (ORDER BY sum(quantity) DESC)
    FROM lakehouse.silver.orders GROUP BY product
""")
show("top_products snapshots", 'SELECT snapshot_id, committed_at, operation FROM lakehouse.gold."top_products$snapshots" ORDER BY committed_at')
first = sql('SELECT snapshot_id FROM lakehouse.gold."top_products$snapshots" ORDER BY committed_at LIMIT 1').iloc[0, 0]
show("time travel (first snapshot)", f"SELECT count(*) AS products FROM lakehouse.gold.top_products FOR VERSION AS OF {first}")
show("current", "SELECT count(*) AS products FROM lakehouse.gold.top_products")
