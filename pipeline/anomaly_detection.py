from pyspark.sql import Window
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, sum, count
from pyod.models.iforest import IForest
from pyod.models.pca import PCA

import pandas as pd 
import matplotlib.pyplot as plt  
from collections import Counter
import numpy as np
from datetime import datetime, timedelta
from sklearn.cluster import KMeans
import json
clf_pca = PCA()
def extract_feature_spark(df, by_wallet= False):
    days = lambda i: i * 86400
    # Define the window specification
    if by_wallet:
        window_spec = Window().partitionBy('address').orderBy(F.col("Time").cast('long')).rangeBetween(-days(5), 0)
    else: 
        window_spec = Window().partitionBy('name').orderBy(F.col("Time").cast('long')).rangeBetween(-days(5), 0)

    # Calculate the rolling sum for the previous 5 days
    df = df.withColumn('sum_5days', sum('price_in_usd').over(window_spec))

    # Calculate the rolling count for the previous 5 days
    df = df.withColumn('count_5days', count('price_in_usd').over(window_spec))

    # Show the resulting DataFrame
    # df.show()
    
    return df

def extract_feature(df, by_wallet= False):
    df = df.set_index('Time').sort_index()
    print(df)

    if not by_wallet:
        df['sum_5days'] = df.groupby('name')['price_in_usd'].transform(lambda s: s.rolling(timedelta(days=5)).sum())
        df['count_5days'] = df.groupby('name')['price_in_usd'].transform(lambda s: s.rolling(timedelta(days=5)).count())
    else:
        df['sum_5days'] = df.groupby('address')['price_in_usd'].transform(lambda s: s.rolling(timedelta(days=5)).sum())
        df['count_5days'] = df.groupby('address')['price_in_usd'].transform(lambda s: s.rolling(timedelta(days=5)).count())
    
    return df

def detect(df, model_name, spark, anomaly_proportion=0.1, by_wallet=False):
    if spark:
        df = extract_feature_spark(df, by_wallet)
        # Convert PySpark DataFrame to pandas DataFrame
        pandas_df = df.toPandas()
    else:
        pandas_df = extract_feature(df, by_wallet)
    
    # train IForest detector
    if model_name == 'IForest':
        clf = IForest(contamination=anomaly_proportion)
    elif model_name == 'PCA':
        clf = PCA(contamination=anomaly_proportion)
    else:
        print('Model was not supported')
        return

    X = pandas_df[['count_5days', 'sum_5days']]
    clf.fit(X)

    # get the prediction labels and outlier scores of the training data
    pandas_df['y_pred'] = clf.labels_ # binary labels (0: inliers, 1: outliers)
    pandas_df['y_scores'] = clf.decision_scores_ # raw outlier scores. The bigger the number the greater the anomaly
    # print(pandas_df.sort_values(by=['y_pred'], ascending=False).head(15))
    
    return pandas_df

def alert_msg(df_detect):
    anomaly = df_detect[df_detect['y_pred'] == 1]
    return len(anomaly)
# def ensemble_detection(df):
def evaluate_scam(df, scam_groundtruth):
    inference1 = df[df['to_address'].apply(lambda x: any([k == x for k in scam_groundtruth]))]
    inference2 = df[df['from_address'].apply(lambda x: any([k == x for k in scam_groundtruth]))]
    detected = list(inference1[inference1['y_pred'] == 1]['to_address']) + list(inference2[inference2['y_pred'] == 1]['from_address'])
    
    set_detected = set(detected)
    # no_detected = len(set_detected)
    
    return set_detected

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
# def ensemble_detection(df):
    