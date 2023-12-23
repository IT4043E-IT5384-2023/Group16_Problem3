import time
from matplotlib import pyplot as plt
import pandas as pd
import numpy as np
from sklearn.preprocessing import RobustScaler
from sklearn.cluster import DBSCAN
from PyNomaly import loop
# from pyspark.sql.functions import count, mean, max, min, datediff, countDistinct
import pyspark.sql.functions as F



def read_data_csv(path):
    df = pd.read_csv(path).drop_duplicates().reset_index().drop(['index'], axis=1)
    # new_df = df_1month.copy()
    df['item_timestamp'] = pd.to_datetime(df['item_timestamp'])
    return df
def extract_node_feature(df):
    graph1 = pd.DataFrame(df.groupby(by=['to_address'])['from_address'].count())
    graph1['value'] = df.groupby(by=['to_address'])['value'].mean()
    graph1['unique_out_degree'] = df.groupby(by=['to_address'])['from_address'].nunique()
    graph1['interval_outgoing'] = df.groupby(by=['to_address'])['timestamp'].agg(lambda x: x.max() - x.min())
    graph1 = graph1.reset_index().rename(columns={'to_address': 'address','from_address': 'out_degree', 'value': 'mean_value_outgoing'})
    
    graph2 = pd.DataFrame(df.groupby(by=['from_address'])['to_address'].count())
    graph2['value'] = df.groupby(by=['from_address'])['value'].mean()
    graph2['unique_in_degree'] = df.groupby(by=['from_address'])['to_address'].nunique()
    graph2['interval_ingoing'] = df.groupby(by=['from_address'])['timestamp'].agg(lambda x: x.max() - x.min())
    graph2 = graph2.reset_index().rename(columns={'from_address': 'address', 'to_address': 'in_degree', 'value': 'mean_value_ingoing'})

    df_nodes = pd.merge(graph1, graph2, how = 'left', on=['address'], validate="many_to_many").fillna(0)

    return df_nodes
def extract_node_feature_spark(df_spark):
    # Count incoming edges per address
    graph1_df = df_spark.groupBy('to_address').agg(
        F.count('from_address').alias('in_degree')
    )
    # Calculate average outgoing value per address
    graph1_df = graph1_df.join(
        df_spark.groupBy('to_address').agg(F.mean('price_in_usd').alias('mean_value_ingoing')),
        on='to_address'
    )
    # Count unique outgoing addresses per address
    graph1_df = graph1_df.join(
        df_spark.groupBy('to_address').agg(F.countDistinct('from_address').alias('unique_in_degree')),
        on='to_address'
    )
    graph1_df = graph1_df.join(
        df_spark.groupBy('to_address').agg((F.max('timestamp') - F.min('timestamp')).alias('interval_ingoing')),
        on='to_address'
    )
    graph1_df = graph1_df.withColumnRenamed('to_address', 'address')
    
    # Count incoming edges per address
    graph2_df = df_spark.groupBy('from_address').agg(
        F.count('to_address').alias('out_degree')
    )
    # Calculate average outgoing value per address
    graph2_df = graph2_df.join(
        df_spark.groupBy('from_address').agg(F.mean('price_in_usd').alias('mean_value_outgoing')),
        on='from_address'
    )
    # Count unique outgoing addresses per address
    graph2_df = graph2_df.join(
        df_spark.groupBy('from_address').agg(F.countDistinct('from_address').alias('unique_out_degree')),
        on='from_address'
    )
    graph2_df = graph2_df.join(
        df_spark.groupBy('from_address').agg((F.max('timestamp') - F.min('timestamp')).alias('interval_outgoing')),
        on='from_address'
    )
    graph2_df = graph2_df.withColumnRenamed('from_address', 'address')
    
    df_nodes_spark = (
        graph1_df
        .join(graph2_df, on='address', how='left_outer')
        .fillna(0)  # Replace null values with 0
    )

    df_nodes = df_nodes_spark.toPandas()
    return df_nodes
def transform_feature(df):
    scaled_df = df.copy()

    scaled_df['in_degree'] = np.log1p(scaled_df['in_degree'])
    scaled_df['out_degree'] = np.log1p(scaled_df['out_degree'])
    scaled_df['mean_value_ingoing'] = np.log1p(scaled_df['mean_value_ingoing'])
    scaled_df['mean_value_outgoing'] = np.log1p(scaled_df['mean_value_outgoing'])
    scaled_df['unique_in_degree'] = np.log1p(scaled_df['unique_in_degree'])
    scaled_df['unique_out_degree'] = np.log1p(scaled_df['unique_out_degree'])
    scaled_df['interval_ingoing'] = np.log1p(scaled_df['interval_ingoing'])
    scaled_df['interval_outgoing'] = np.log1p(scaled_df['interval_outgoing'])

    # RobustScaler is less prone to outliers
    rob_scaler = RobustScaler()
    scaled_df['in_degree'] = rob_scaler.fit_transform(scaled_df['in_degree'].values.reshape(-1, 1))
    scaled_df['out_degree'] = rob_scaler.fit_transform(scaled_df['in_degree'].values.reshape(-1, 1))
    scaled_df['mean_value_ingoing'] = rob_scaler.fit_transform(scaled_df['mean_value_ingoing'].values.reshape(-1, 1))
    scaled_df['mean_value_outgoing'] = rob_scaler.fit_transform(scaled_df['mean_value_outgoing'].values.reshape(-1, 1))
    scaled_df['unique_in_degree'] = rob_scaler.fit_transform(scaled_df['unique_in_degree'].values.reshape(-1, 1))
    scaled_df['unique_out_degree'] = rob_scaler.fit_transform(scaled_df['unique_out_degree'].values.reshape(-1, 1))
    scaled_df['interval_ingoing'] = rob_scaler.fit_transform(scaled_df['interval_ingoing'].values.reshape(-1, 1))
    scaled_df['interval_outgoing'] = rob_scaler.fit_transform(scaled_df['interval_outgoing'].values.reshape(-1, 1))
    
    return scaled_df

def detect_mixing(df):
    scaled_df = transform_feature(df)
    db = DBSCAN(eps=0.9, min_samples=10).fit(scaled_df[['out_degree', 'mean_value_outgoing', 'unique_out_degree',
       'interval_outgoing', 'in_degree', 'mean_value_ingoing',
       'unique_in_degree', 'interval_ingoing']])
    scaled_df['cluster'] = db.labels_
    
    min_neighbor = min(scaled_df['cluster'].value_counts())
    
    m = loop.LocalOutlierProbability(scaled_df[['out_degree', 'mean_value_outgoing', 'unique_out_degree',
       'interval_outgoing', 'in_degree', 'mean_value_ingoing',
       'unique_in_degree', 'interval_ingoing']], extent=3, n_neighbors=min_neighbor-1, cluster_labels=list(db.labels_)).fit()
    scores = m.local_outlier_probabilities
    scaled_df['LoOP_scores'] = scores
    # print(scaled_df.sort_values(by=['LoOP_scores'], ascending=False).head(10))
    
    return scaled_df

def handle_detection_mixing(df_detect, threshold = 0.9):
    df_detect['pred'] = df_detect['LoOP_scores'].apply(lambda x: 1 if x > threshold else 0)
    df_detect['label'] = df_detect['LoOP_scores'].apply(lambda x: 'mixing_risk' if x > threshold else 'normal')
    return df_detect[df_detect['label'] == 'mixing_risk']
def alert_msg_mixing(df_detect):
    anomaly = df_detect[df_detect['pred'] == 1]
    # print(anomaly)
    addr_anomaly = list(anomaly['address'])
    # msg = f"Found {len(addr_anomaly)} anomaly address"
    return len(addr_anomaly)
def visualize_anomaly(df, name):
    colors = np.array(['#ff7f00', '#377eb8'])
    plt.scatter(df['mean_value_ingoing'], df['in_degree'], s=10, color=colors[(df['pred'] - 1) // 2])
    plt.legend(('inliers', 'outliers'))
    # plt.xlabel("5-day count of transactions.")
    # plt.ylabel("5-day sum of transactions.")
    plt.title(f"Anomaly Detection - {name}")
    plt.savefig(f"anomaly_detection_{name}.png")
    # plt.show()
def visualize(scaled_df):
    fig = plt.figure(figsize=(7, 7))
    ax = fig.add_subplot(111)

    # Use 'cluster_label' for color and 'LoOP_scores' for size
    # scatter = ax.scatter(scaled_df['in_degree'], scaled_df['mean_value_ingoing'],
                        # c=cluster_labels, cmap='viridis', s=list(scaled_df['LoOP_scores']*100))
    scatter = ax.scatter(scaled_df['in_degree'], scaled_df['mean_value_ingoing'],
                        c=scaled_df['LoOP_scores'], cmap='viridis', s=50)

    # Add a colorbar as a legend for cluster labels
    plt.colorbar(scatter, ax=ax, label='Cluster Label')

    # Set labels for axes
    ax.set_xlabel('In Degree')
    ax.set_ylabel('Value Ingoing')

    # Add a title
    ax.set_title('Scatter Plot with Cluster Labels and LoOP Scores')

    # Show the plot
    plt.savefig("anomaly_detection_clustered.png")

    plt.show()
        
    
def main():
    start_time = time.time()
    print("Cluster-based anomaly detection!")

    path = r"D:\Documents\BigData\Group16_Problem5\data\final_etherium_token_transfer.csv"
    df = read_data_csv(path)
    
    df_nodes = extract_node_feature(df)
    # scaled_df = transform_feature(df_nodes)
    df_detect = detect_mixing(df_nodes)
    df_detect = handle_detection_mixing(df_detect)
    visualize_anomaly(df_detect, "user address")
    print(alert_msg_mixing(df_detect))
    # print(df_detect)
    elapsed_time_seconds = time.time() - start_time

    # Convert elapsed time to minutes and seconds
    elapsed_minutes = int(elapsed_time_seconds // 60)
    elapsed_seconds = int(elapsed_time_seconds % 60)
    
    # visualize(scaled_df)

    print(f"Elapsed time: {elapsed_minutes} minutes and {elapsed_seconds} seconds")
    
if __name__ == '__main__':
    main()
    
    