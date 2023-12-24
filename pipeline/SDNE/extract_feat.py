from .src.models.sdne import SDNE
from .src.models.basemodel import MultiClassifier
from sklearn.linear_model import LogisticRegression

import matplotlib.pyplot as plt
from sklearn.manifold import TSNE
import numpy as np
import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt

def read_graph(csv):
    # Đọc dữ liệu từ file CSV
    if isinstance(csv, str):
        df = pd.read_csv(csv)
    else:
        df = csv

    # Xác định edges và features
    edges = df[['from_address', 'to_address']].values
    # features = df[['value', 'gas', 'gas_price']].values.astype(float)

    # Chuyển đổi địa chỉ thành số nguyên để tạo node index
    all_addresses = set(df['from_address']).union(set(df['to_address']))
    address_to_index = {addr: idx for idx, addr in enumerate(all_addresses)}
    index_to_address = {idx: addr for addr, idx in address_to_index.items()}  # Thêm mapping ngược

    # Tạo đồ thị NetworkX
    G = nx.Graph()

    # Thêm các node vào đồ thị
    for address, index in address_to_index.items():
        G.add_node(index, address=address)

    # Thêm các cạnh vào đồ thị
    edge_list = [(address_to_index[row[0]], address_to_index[row[1]]) for row in edges]
    G.add_edges_from(edge_list)
    return G, address_to_index, index_to_address

def read_node_label(file_path, skip_head=False):
    X, y = [], []
    with open(file_path, "r") as f:
        if skip_head:
            f.readline()
        for line in f.readlines():
            tmp = line.strip().split(" ")
            X.append(tmp[0])
            y.append(tmp[1:])
    return X, y

def plot_embeddings(embeddings, X, y):
    embed_list = []
    for node in X:
        embed_list.append(embeddings[node])
    tsne = TSNE(n_components=2)
    node_tsned = tsne.fit_transform(np.asarray(embed_list), y)
    color_idx = {}
    for i in range(len(X)):
        color_idx.setdefault(y[i][0], [])
        color_idx[y[i][0]].append(i)
    for c, idx in color_idx.items():
        plt.scatter(node_tsned[idx, 0], node_tsned[idx, 1], label=c)
    plt.legend()
    plt.show()

def graph_extract_node_features(csv):
    # G = nx.read_edgelist('./data/wiki/Wiki_edgelist.txt', create_using=nx.DiGraph(), nodetype=None, data=[('weight', int)])
    G, address_to_index, index_to_address = read_graph(csv) # read_graph(csv_path = "/content/drive/MyDrive/HUST/20231/big_data/proj/final_etherium_token_transfer.csv")

    model = SDNE(G, hidden_layers=[256, 128])
    model.fit(batch_size=1024, epochs=100)
    embeddings = model.get_embeddings()


    # Chuyển đổi dictionary thành DataFrame
    df_embeddings = pd.DataFrame(list(embeddings.items()), columns=['address', 'embedding'])

    # Thêm các cột cho từng phần tử trong embedding
    df_embeddings = pd.concat([df_embeddings, pd.DataFrame(df_embeddings['embedding'].to_list(), columns=[f'emb_{i}' for i in range(len(df_embeddings["embedding"].iloc[0]))])], axis=1)
    # Xóa cột 'embedding' nếu bạn không muốn giữ lại cột này
    df_embeddings = df_embeddings.drop('embedding', axis=1)
    return df_embeddings

