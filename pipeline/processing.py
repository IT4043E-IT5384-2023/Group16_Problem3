from .kafka_streaming_service import get_streaming_data
from .cluster_based_anomaly import *
from .anomaly_detection import *
from .elasticsearch_processing import *
import warnings
import os

print(f"{os.path.abspath('./pipeline/final_etherium_token_transfer.csv')=}")

warnings.filterwarnings('ignore')

def anomaly_token_transaction(df_raw):
    # df = extract_feature(df_raw)
    df_detect = detect(df_raw, 'IForest' )
    visualize(df_detect, 'IForest')
    # msg = alert_msg(df_detect)
    df_rules = handle_detection(df_detect)[['name','from_address','to_address','sum_5days','count_5days','y_pred', 'label', 'item_timestamp']]
    # df_rules.to_csv("detected_risk.csv")
    print('anomaly token transaction:', df_rules.columns)
    return df_rules
def anomaly_wallet_transaction(df_raw):
    ### Group by user transaction
    df1 = df_raw[['from_address', 'transaction_hash', 'price_in_usd', 'item_timestamp', 'Time']].rename(columns={'from_address': 'address'})
    df2 = df_raw[['to_address', 'transaction_hash', 'price_in_usd', 'item_timestamp', 'Time']].rename(columns={'to_address': 'address'})
    df_wallets = pd.concat([df1, df2])
    df_wallets_detect = detect(df_wallets, 'IForest', 0.002, by_wallet=True)
    visualize(df_wallets_detect, 'IForest')
    # msg = alert_msg(df_wallets_detect)
    df_rules = handle_detection(df_wallets_detect, wallets=True)[['transaction_hash','address','price_in_usd','sum_5days','count_5days','y_pred', 'label', 'item_timestamp']]
    # df_rules.to_csv("wallets_detected_risk.csv")
    print('anomaly wallet transaction:', df_rules.columns)
    return df_rules
    
def mixing_service(df_raw):
    df_nodes = extract_node_feature(df_raw)
    # scaled_df = transform_feature(df_nodes)
    df_detect = detect_mixing(df_nodes)
    df_detect = handle_detection_mixing(df_detect)
    visualize_anomaly(df_detect, "user address")
    # msg = alert_msg_mixing(df_detect)
    return df_detect

def write_to_bucket(df):
    df.to_csv("stream_data.csv")
def load_data():
    df = pd.read_csv('stream_data.csv')
    df['Time'] = pd.to_datetime(df['item_timestamp'])
    df = df[['name', 'transaction_hash', 'from_address', 'to_address', 'timestamp', 'item_timestamp', 'value', 'price_in_usd', 'Time']]
    return df
def get_stream_data(use_kafka=False):
    # use_kafka = False
    if use_kafka:
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
            return
    else: 
        path = "./pipeline/final_etherium_token_transfer.csv"
        
        print(os.path.abspath(path))
        df_raw = read_data_csv(path)
        
    return df_raw
def process():
    use_kafka = True
    df_raw = get_stream_data(use_kafka)
    # df_raw.to_csv("stream_data.csv", index=False)
    url = "http://34.143.255.36:9200/"
    user_name = 'elastic'
    pwd = 'elastic2023'
    es = initialize_elasticsearch(url, user_name, pwd)
    df1 = anomaly_token_transaction(df_raw)
    index_document(es, "g16_token_anomaly_", df1)
    df2 = anomaly_wallet_transaction(df_raw)
    index_document(es, "g16_wallets_anomaly_", df2)
    df3 = mixing_service(df_raw)
    index_document(es, "g16_mixing_service_", df3)

if __name__ == '__main__':
    process()