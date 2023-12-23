from pymongo import MongoClient
import psycopg2
from psycopg2 import InterfaceError, OperationalError
import pandas as pd
from datetime import datetime
import numpy as np
import json
import pickle
from kafka import KafkaProducer
import time
import socket

from kafka_streaming_service import *

# query token_price info from smart_contract in knowledge graph
def query_list_token_info(db_kg, token_list_name, token_list_symbol):
    print("Query token info ...")
    table_smartContract = db_kg.smart_contracts
    
    eth_smart_contracts_query = table_smartContract.find({'chainId': "0x1", 'name': {'$in': token_list_name}, 'symbol': {'$in': token_list_symbol[2:3]}},{"address": 1, 'idCoingecko': 1, "symbol": 1, "name": 1, "decimals": 1, "priceChangeLogs": 1})
    eth_smart_contracts = list(eth_smart_contracts_query)
    df_token_info = pd.DataFrame(eth_smart_contracts).rename(columns={'address': 'contract_address'})
    return df_token_info

def query_token_info(db_kg, token_name, token_symbol):
    print("Query token info ...")
    table_smartContract = db_kg.smart_contracts
    
    eth_smart_contracts_query = table_smartContract.find({'chainId': "0x1", 'name': token_name, 'symbol': token_symbol},{"address": 1, "name": 1, "decimals": 1, "priceChangeLogs": 1})
    eth_smart_contracts = list(eth_smart_contracts_query)
    df_token_info = pd.DataFrame(eth_smart_contracts).rename(columns={'address': 'contract_address'})
    return df_token_info

# query token_transfer table from postgre
def query_token_transfer(db_postgre, token_address):
    print("Query token transfer ...")
    cur = db_postgre.cursor()
    results = []
    for address in token_address:
        # print('address: ', address)
        try:
            cur.execute(f"SELECT * FROM chain_0x1.token_transfer WHERE contract_address = '{address}'")
        except InterfaceError:
            db_postgre = psycopg2.connect(database = "postgres",
                        user = "student_token_transfer",
                        host= '34.126.75.56',
                        password = "svbk_2023",
                        port = 5432)
            cur = db_postgre.cursor()
            cur.execute(f"SELECT * FROM chain_0x1.token_transfer WHERE contract_address = '{address}'")
        print("The number of parts: ", cur.rowcount)
        results.extend(cur.fetchall())
    # print('result: .................', results)
    column_names = [desc[0] for desc in cur.description]
    # print(column_names)
    df_token_transfer = pd.DataFrame(results, columns = column_names)
    return df_token_transfer

def query_blocks(db_mongo, block_list):
    print("Query block ...")
    table_blocks = db_mongo.blocks
    results = []
    for block in block_list:
        query1 = { 'number': block} 
        query2 = { 'number': 1, 'timestamp': 1, 'item_timestamp': 1, '_id': 0 }
        results.extend(list(table_blocks.find(query1, query2)))
    df_blocks = pd.DataFrame(results).rename(columns={'number': 'block_number'})
    return df_blocks

def get_price(sample):

  if isinstance(sample['priceChangeLogs'], float)  :
    return 0
  elif not bool(sample['priceChangeLogs']):
#     print(sample['transaction_hash'])
    return 0
  else:
    test_dict = sample['priceChangeLogs']
    search_key = sample['timestamp']

    res = test_dict.get(search_key) or test_dict[
          min(test_dict.keys(), key = lambda key: abs(int(key)-search_key))]
    return res

def create_dataset(df_token_price_merged):
    dataset = df_token_price_merged.copy()
    dataset['item_timestamp'] = dataset['timestamp'].apply(datetime.fromtimestamp).astype(str)
    dataset['price'] = dataset.apply(get_price, axis=1)
    dataset['price_in_usd'] = dataset['price']*dataset['value']
    if len(dataset[dataset['price'] > 5000000][['name', 'price']]) > 0:
        print('tokens with price greater than $5 million')
        print(dataset[dataset['price'] > 5000000][['name', 'price']])
    
    return dataset[dataset['price'] > 0][['contract_address','transaction_hash','from_address','to_address','value','name','timestamp','item_timestamp','price','price_in_usd']]

def write_to_file(df, file_name):
    df['item_timestamp'] = df['item_timestamp'].astype(str)
    data = df.to_dict('split')
    with open(f"{file_name}.json", "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

def write_to_kafka(kafka_url, topic, df, file_name):
    producer = KafkaProducer(bootstrap_servers=[kafka_url])
    print(f"config: {producer.config}")
    print(f"connect: {producer.bootstrap_connected()}")
    data = df.to_dict('split')
    data["id"] = file_name
    producer.send(topic, pickle.dumps(data))
def main():
    start_time = time.time()
    # Provide the mongodb atlas url to connect python to mongodb using pymongo
    CONNECTION_STRING = "mongodb://etlReaderAnalysis:etl_reader_analysis__Gr2rEVBXyPWzIrP@34.126.84.83:27017,34.142.204.61:27017,34.142.219.60:27017"

    # Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
    client = MongoClient(CONNECTION_STRING)
    db_mongo = client['ethereum_blockchain_etl']

    # Knowledge Graph db
    CONNECTION_STRING_KG = "mongodb://klgReaderAnalysis:klgReaderAnalysis_4Lc4kjBs5yykHHbZ@35.198.222.97:27017,34.124.133.164:27017,34.124.205.24:27017"
    client_kg = MongoClient(CONNECTION_STRING_KG)
    db_kg = client_kg['knowledge_graph']

    db_postgre = psycopg2.connect(database = "postgres",
                            user = "student_token_transfer",
                            host= '34.126.75.56',
                            password = "svbk_2023",
                            port = 5432)
    
    df_addr = pd.read_csv(r'D:\Documents\BigData\token\top_token.csv')
    token_list_name = list(df_addr['name'])
    token_list_symbol = list(df_addr['symbol'])
    
    df_token_info = query_token_info(db_kg, token_list_name[2], token_list_symbol[2])
    print("token_info: ", df_token_info)
    token_list = ['eth', 'usdt', 'bnb', 'usdc', 'steth', 'ton', 'link', 'matic', 'wbtc', 'dai', 'shib', 'uni', 'leo', 'okb', 'tusd', 'cro', 'ldo', 'busd', 'mnt', 'qnt']
    # df_token_info = df_token_info[df_token_info['symbol'].apply(lambda x: any([k == x for k in token_list]))]
    
    token_address = tuple(df_token_info['contract_address'])
    # print('token_address: ', token_address)
    df_token_transfer = query_token_transfer(db_postgre, token_address)
    print('token_transfer: ', df_token_transfer)
    
    df_token_merged = pd.merge(df_token_transfer, df_token_info, how="left", on=["contract_address"])
    print(df_token_merged)
    # df_token_merged.to_csv("etherium_token_transfer.csv", index=False)
    
    block_list = list(df_token_merged['block_number'])

    df_blocks = query_blocks(db_mongo, block_list)
    df_token_price_merged = pd.merge(df_token_merged, df_blocks, how="left", on=["block_number"])
    # print(df_token_price_merged)
    dataset = create_dataset(df_token_price_merged)
    # dataset.to_csv("final_etherium_token_transfer.csv", index=False)
    # print(dataset)
    producer_config = {
        'bootstrap.servers': '34.142.194.212:9092',
        'client.id': socket.gethostname()
    }
    topic='group16_stream'
    publish_to_kafka(producer_config, topic, df=dataset)
    # write_to_kafka("34.142.194.212:9092", "group16_bnb", dataset, "test_pipeline")
    # write_to_file(dataset, "group16_bnb")
    consumer_config = {
        'bootstrap.servers': '34.142.194.212:9092',
        'group.id': 'kafka-consumer',
        'auto.offset.reset': 'earliest'  # You can change this to 'latest' if you want to start reading from the latest offset
    }
    # get_streaming_data(consumer_config, topic)
    # Calculate the elapsed time in seconds
    elapsed_time_seconds = time.time() - start_time

    # Convert elapsed time to minutes and seconds
    elapsed_minutes = int(elapsed_time_seconds // 60)
    elapsed_seconds = int(elapsed_time_seconds % 60)

    print(f"Elapsed time: {elapsed_minutes} minutes and {elapsed_seconds} seconds")
    print("Finish!")

if __name__ == '__main__':
    main()


