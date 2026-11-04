def attempt(label, sql):
    try:
        spark.sql(sql).show()
        print(f"[OK] {label}")
    except Exception as e:
        lines = [l.strip() for l in str(e).splitlines() if "orbidden" in l or "not authorized" in l]
        print(f"[REFUSED] {label}:", (lines or [str(e)[:300]])[0][:400])


attempt("show tables", "SHOW TABLES IN polaris.silver")
attempt("read orders by product", "SELECT product, count(*) AS n FROM polaris.silver.orders GROUP BY product")
attempt("INSERT into orders", "INSERT INTO polaris.silver.orders SELECT * FROM polaris.silver.orders LIMIT 1")
attempt("ALTER users", "ALTER TABLE polaris.silver.users ADD COLUMN phone STRING")
