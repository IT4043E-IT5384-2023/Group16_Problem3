import pandas as pd 
import matplotlib.pyplot as plt  
import seaborn as sns 
from collections import Counter
import numpy as np
from datetime import datetime, timedelta
from pyod.models.iforest import IForest
from pyod.models.pca import PCA
from sklearn.cluster import KMeans
import json

def read_data_json(path):
    """Reads a JSON file and returns the data in DataFrame format."""
    # Read the json file
    with open(path) as f:
        data = json.load(f)
        return pd.DataFrame(data)
    
def read_data_csv(path):
    # path = r"D:\Documents\BigData\data\final_etherium_top_token_transfer_1month.csv"
    df = pd.read_csv(path).drop_duplicates().reset_index().drop(['index'], axis=1)
    df['Time'] = pd.to_datetime(df['item_timestamp'])
    df = df[['name', 'transaction_hash', 'from_address', 'to_address', 'timestamp', 'item_timestamp', 'value', 'price_in_usd', 'Time']]
    return df

def wallet_most_transaction(df):
    
    accounts = df['from_address'].to_list() + df['to_address'].to_list()
    most_common = Counter(accounts).most_common()
    
    return most_common

def wash_check(df, addr):
    # wash_check = df[['from_address', 'to_address']]
    # dupes = wash_check[wash_check.duplicated(['from_address'])]
    wash_check = df[(df['from_address'] == addr) | (df['to_address'] == addr)]
    return wash_check
def extract_feature(df, by_wallet= False):
    df = df.set_index('Time').sort_index()

    if not by_wallet:
        df['sum_5days'] = df.groupby('name')['price_in_usd'].transform(lambda s: s.rolling(timedelta(days=5)).sum())
        df['count_5days'] = df.groupby('name')['price_in_usd'].transform(lambda s: s.rolling(timedelta(days=5)).count())
    else:
        df['sum_5days'] = df.groupby('address')['price_in_usd'].transform(lambda s: s.rolling(timedelta(days=5)).sum())
        df['count_5days'] = df.groupby('address')['price_in_usd'].transform(lambda s: s.rolling(timedelta(days=5)).count())
    
    return df

def detect(df, model_name, anomaly_proportion=0.1, by_wallet=False):
    df = extract_feature(df, by_wallet)
    # train IForest detector
    if model_name == 'IForest':
        clf = IForest(contamination=anomaly_proportion)
    elif model_name == 'PCA':
        clf = PCA(contamination=anomaly_proportion)
    else:
        print('Model was not supported')
        return

    X = df[['count_5days', 'sum_5days']]
    clf.fit(X)

    # get the prediction labels and outlier scores of the training data
    df['y_pred'] = clf.labels_ # binary labels (0: inliers, 1: outliers)
    df['y_scores'] = clf.decision_scores_ # raw outlier scores. The bigger the number the greater the anomaly
    # print(pandas_df.sort_values(by=['y_pred'], ascending=False).head(15))
    
    return df

# def ensemble_detection(df):
def evaluate_scam(df, scam_groundtruth):
    inference1 = df[df['to_address'].apply(lambda x: any([k == x for k in scam_groundtruth]))]
    inference2 = df[df['from_address'].apply(lambda x: any([k == x for k in scam_groundtruth]))]
    detected = list(inference1[inference1['y_pred'] == 1]['to_address']) + list(inference2[inference2['y_pred'] == 1]['from_address'])
    
    set_detected = set(detected)
    # no_detected = len(set_detected)
    
    return set_detected
def alert_msg(df_detect):
    anomaly = df_detect[df_detect['y_pred'] == 1]
    # print(anomaly)
    # addr_anomaly = list(anomaly['transaction_hash'])
    # msg = f"Found {len(addr_anomaly)} anomaly transactions"
    return len(anomaly)
    
def visualize(df, name):
    colors = np.array(['#ff7f00', '#377eb8'])
    plt.scatter(df['count_5days'], df['sum_5days'], s=10, color=colors[(df['y_pred'] - 1) // 2])
    plt.legend(('outliers', 'inliers'))
    plt.xlabel("5-day count of transactions.")
    plt.ylabel("5-day sum of transactions.")
    plt.title(f"Anomaly Detection - {name}")
    plt.savefig(f"anomaly_detection_{name}.png")
    # plt.show()

def label(row):
    if row['cluster'] > 0:
        return 'high_risk'
    elif (row['cluster'] == 0) and (row['count_5days'] > 1000):
        return 'wash_risk'
    else:
        return 'human_in_loop_risk'
    
def label_wallets(row):
    if row['cluster'] == 0:
        return 'high_risk'
    else:
        return 'wash_risk'
        
def handle_detection(df_detect, wallets=False):

    df_rules = df_detect[df_detect['y_pred'] == 1]
    kmeans = KMeans(n_clusters=2)
    kmeans.fit(df_rules[['count_5days', 'sum_5days']])
    df_rules['cluster'] = kmeans.labels_
    if not wallets:
        df_rules['label'] = df_rules.apply(label, axis=1)
    else: 
        df_rules['label'] = df_rules.apply(label_wallets, axis=1)

    df_detect['label'] = 'normal'
    for index, row in df_rules.iterrows():
        df_detect.loc[index, 'label'] = row['label']

    return df_detect
