import duckdb
from extractor import CoinDataExtractor
from datetime import datetime

conn = duckdb.connect('data/CoinCap.db')
print(conn.execute('SELECT * FROM history_price').fetchdf())
# #conn.execute('DROP TABLE history_price')

#print(conn.execute('Show tables').fetchdf())
# conn.close()







#first step
# from initialize_warehouse import create_tables, insert_into_date_table
# from fetch_all_assets import load_assets


# create_tables()
# insert_into_date_table()
# load_assets()


#second step
# coin_list = ['binance-coin','bitcoin','ethereum']

# for asset in coin_list:
#     coin_data_extractor = CoinDataExtractor(asset)
#     coin_data_extractor.load_data_from_minio_to_duckdb()


