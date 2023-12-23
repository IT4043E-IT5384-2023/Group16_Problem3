from spark_processing import *
from anomaly_detection import *
from cluster_based_anomaly import *
from elasticsearch_processing import *
from kafka_streaming_service import get_streaming_data

import warnings

warnings.filterwarnings('ignore')

# Show the resulting DataFrame
# df_test.show()
def anomaly_token_transaction(df_raw, spark):
    df_detect = detect(df_raw, 'IForest', spark )
    # visualize(df_detect, 'IForest')
    # print(alert_msg(df_detect))
    df_rules = handle_detection(df_detect)[['name','from_address','to_address','sum_5days','count_5days','y_pred', 'label', 'item_timestamp']]
    # df_rules.to_csv("detected_risk.csv")
    print('anomaly token transaction:', df_rules)
    return df_rules
def anomaly_wallet_transaction(df_raw, spark):
    ### Group by user transaction
    if spark:
        # Select relevant columns, rename 'from_address' to 'address', and add 'Time' column
        df1_spark = df_raw.select(col("from_address").alias("address"),'name', 'transaction_hash', "price_in_usd", 'item_timestamp', "Time")

        # Select relevant columns, rename 'to_address' to 'address', and add 'Time' column
        df2_spark = df_raw.select(col("to_address").alias("address"), 'name', 'transaction_hash', "price_in_usd", 'item_timestamp', "Time")

        # Union the two DataFrames
        df_wallets_spark = df1_spark.union(df2_spark)

        # Convert 'Time' column to timestamp type
        df_wallets_spark = df_wallets_spark.withColumn("Time", df_wallets_spark["Time"].cast("timestamp"))

        # Sort the DataFrame by 'Time' column
        df_wallets = df_wallets_spark.orderBy("Time")
    else:
        ### Group by user transaction
        df1 = df_raw[['from_address', 'transaction_hash', 'price_in_usd', 'item_timestamp', 'Time']].rename(columns={'from_address': 'address'})
        df2 = df_raw[['to_address', 'transaction_hash', 'price_in_usd', 'item_timestamp', 'Time']].rename(columns={'to_address': 'address'})
        df_wallets = pd.concat([df1, df2])
    
    df_wallets_detect = detect(df_wallets, 'IForest', spark, 0.002, by_wallet=True)
    # visualize(df_wallets_detect, 'IForest')
    # print(alert_msg(df_wallets_detect))
    df_rules = handle_detection(df_wallets_detect, wallets=True)[['transaction_hash','address','price_in_usd','sum_5days','count_5days','y_pred', 'label', 'item_timestamp']]
    # df_rules.to_csv("wallets_detected_risk.csv")
    # df_rules.to_csv("wallets_detected_risk.csv")
    print('anomaly wallet transaction:', df_rules)
    return df_rules
def mixing_service(df_raw, spark):
    if spark:
        df_nodes = extract_node_feature_spark(df_raw)
    else:
        df_nodes = extract_node_feature(df_raw)
    # scaled_df = transform_feature(df_nodes)
    df_detect = detect_mixing(df_nodes)
    df_detect = handle_detection_mixing(df_detect)
    # visualize_anomaly(df_detect, "user address")
    # print(alert_msg_mixing(df_detect))
    return df_detect

def write_to_bucket(df):
    df.to_csv("stream_data.csv")

def get_stream_data():
    # use_kafka = False
    topic='group16_stream'
    consumer_config = {
        'bootstrap.servers': '34.142.194.212:9092',
        'group.id': 'kafka-consumer',
        'auto.offset.reset': 'earliest'  # You can change this to 'latest' if you want to start reading from the latest offset
    }
    df_raw = get_streaming_data(consumer_config, topic)
    if df_raw is not None:
        df_raw['Time'] = pd.to_datetime(df_raw['item_timestamp'])
        write_to_bucket(df_raw)
    else:
        print('No data received!')
        
def get_dataset(bucket=False):

    if bucket: 
        data_path=f"gs://it4043e-it5384/it4043e/it4043e_group16_problem5/final_etherium_token_transfer.csv"
        bucket_license = "/opt/bucket_connector/lucky-wall-393304-3fbad5f3943c.json"
        spark = initialize_spark_session("Group16_", "spark://34.142.194.212:7077")
        df_spark = get_dataframe(spark, bucket_license, data_path)

        df_test = df_spark.select('contract_address', 'name', 'transaction_hash', 'from_address', 'to_address', 'timestamp', 'item_timestamp', 'value', 'price_in_usd', 'Time')
        # Sort the DataFrame by the 'Time' column
        df_raw = df_test.orderBy('Time')
      
    else:
        df_raw = pd.read_csv('stream_data.csv')
        df_raw['Time'] = pd.to_datetime(df_raw['item_timestamp'])
        df_raw = df_raw[['name', 'transaction_hash', 'from_address', 'to_address', 'timestamp', 'item_timestamp', 'value', 'price_in_usd', 'Time']]
    return df_raw

def process():
    # get_stream_data()
    spark_ = False
    df_raw = get_dataset(spark_)
    # print(df_raw)
    # df_raw.to_csv("stream_data.csv", index=False)
    url = "http://34.143.255.36:9200/"
    user_name = 'elastic'
    pwd = 'elastic2023'
    es = initialize_elasticsearch(url, user_name, pwd)
    
    df1 = anomaly_token_transaction(df_raw, spark_)
    index_document(es, "g16_spark_token_anomaly1", df1)
    df2 = anomaly_wallet_transaction(df_raw, spark_)
    index_document(es, "g16_spark_wallets_anomaly1", df2)
    df3 = mixing_service(df_raw, spark_)
    index_document(es, "g16_spark_mixing_service1", df3)

    
if __name__ == '__main__':
    process()
# df_test[['y_pred_pca', 'y_scores_pca']] = df_pca
# df_test[['y_pred_if', 'y_scores_if']] = df_if
# print(df_test)
# # Show the resulting DataFrame
# df_spark.show()