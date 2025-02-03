import duckdb

conn = duckdb.connect()  # or just duckdb.connect() for in-memory
result = conn.execute("SHOW TABLES").fetchall()
for row in result:
    print(row)