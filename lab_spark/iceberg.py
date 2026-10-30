from pyspark.sql import functions as F

T = "lab.silver.orders"

print("== snapshots")
spark.sql(f"SELECT snapshot_id, operation, committed_at FROM {T}.snapshots ORDER BY committed_at").show(truncate=False)

print("== files")
spark.sql(f"SELECT file_path, file_format, record_count, file_size_in_bytes FROM {T}.files").show(truncate=False)

print("== append of 3 new orders")
stg_orders = spark.table(T)
new_orders = stg_orders.limit(3).withColumn("order_id", F.expr("uuid()"))
new_orders.writeTo(T).append()

print("== snapshots after append")
spark.sql(f"SELECT snapshot_id, operation, committed_at FROM {T}.snapshots ORDER BY committed_at").show(truncate=False)
print("rows now:", spark.table(T).count())

print("== time travel")
snapshots = spark.sql(f"SELECT snapshot_id FROM {T}.snapshots ORDER BY committed_at").collect()
first_snapshot = snapshots[0]["snapshot_id"]
print("first snapshot:", first_snapshot)
print("rows at first snapshot:", spark.sql(f"SELECT count(*) AS n FROM {T} VERSION AS OF {first_snapshot}").collect()[0]["n"])
print("rows at current:", spark.sql(f"SELECT count(*) AS n FROM {T}").collect()[0]["n"])

print("== schema evolution")
spark.sql(f"ALTER TABLE {T} ADD COLUMN source STRING")
spark.sql(f"DESCRIBE {T}").show()
spark.sql(f"INSERT INTO {T} SELECT order_id, user_id, ordered_at, quantity, product, 'spark' FROM {T} LIMIT 1")
spark.sql(f"SELECT * FROM {T} WHERE source IS NOT NULL").show(truncate=False)
spark.sql(f"SELECT snapshot_id, operation, committed_at FROM {T}.snapshots ORDER BY committed_at").show(truncate=False)
