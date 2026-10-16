import re
from operator import add

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("lab-rdd").master("local[*]").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

users_lines = sc.textFile("users.csv")
orders_lines = sc.textFile("orders.csv")

header = orders_lines.first()
orders_data = orders_lines.filter(lambda line: line != header and line.strip() != "")

UUID_AT_START = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
users_records = users_lines.filter(lambda line: UUID_AT_START.match(line) is not None)


def parse_order(line):
    order_id, user_id, date, quantity, product = line.split(",")
    return {
        "order_id": order_id,
        "user_id": user_id,
        "date": date,
        "quantity": int(quantity),
        "product": product.strip().lower(),
    }


orders_rdd = orders_data.map(parse_order).cache()

user_quantity = orders_rdd.map(lambda o: (o["user_id"], o["quantity"])).reduceByKey(add)


def most_recent(a, b):
    return a if a["date"] > b["date"] else b


orders_dedup = (
    orders_rdd.map(lambda o: (o["order_id"], o))
    .reduceByKey(most_recent)
    .values()
)

users_kv = users_records.map(lambda line: tuple(line.split(",")[:2]))
orders_kv = orders_rdd.map(lambda o: (o["user_id"], o))
enriched = orders_kv.join(users_kv)

print("users lines", users_lines.count(), "users", users_records.count())
print("orders", orders_rdd.count(), "dedup", orders_dedup.count())
print("quantity per user", user_quantity.take(5))
print("enriched", enriched.count())

spark.stop()
