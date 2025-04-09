import duckdb

conn = duckdb.connect("data/historical_bitcoin.db")
#fetch data

#conn.execute("DROP TABLE bitcoin")
print(conn.execute("SELECT priceUSD, circulatingSupply, date FROM bitcoin ORDER BY date asc").fetchdf())

#print(conn.execute("DESCRIBE bitcoin").fetchdf())
conn.close()


