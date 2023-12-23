from elasticsearch import Elasticsearch
import json
def initialize_elasticsearch(url, user_name, pwd):
    es = Elasticsearch(
        url,  # Elasticsearch endpoint
        basic_auth=(user_name, pwd),  # API key ID and secret
    )
    return es

def index_document(es, index_name, df):
    # data = df.to_dict(orient = 'list') 
    for index, row in df.iterrows():
        data = row.to_dict()
        data['Time'] = index
        # print(data)
        # json_data = json.dumps(data)
        # document['Time'] = df_test.index[i]
        resp = es.index(index=index_name, id = index, document=data)

    print(resp['result'])
    
if __name__ == '__main__':
    url = "http://34.143.255.36:9200/"
    user_name = 'elastic'
    pwd = 'elastic2023'
    es = initialize_elasticsearch(url, user_name, pwd)
    print(es.info())