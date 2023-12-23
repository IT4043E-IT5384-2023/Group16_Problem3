import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType

# Initialize logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
logger = logging.getLogger("spark_structured_streaming")

def initialize_spark_session(app_name, url):
    """
    Initialize the Spark Session with provided configurations.
    
    :param app_name: Name of the spark application.
    :param access_key: Access key for S3.
    :param secret_key: Secret key for S3.
    :return: Spark session object or None if there's an error.
    """
    try:
        ##Configuration

        #config the connector jar file
        spark = (SparkSession.builder.appName(app_name).master(url)
                 .config("spark.jars", "/opt/spark/jars/gcs-connector-latest-hadoop2.jar")
                 .config("spark.executor.memory", "2G")  #excutor excute only 2G
                .config("spark.driver.memory","4G") 
                .config("spark.executor.cores","1") #Cluster use only 3 cores to excute as it has 3 server
                .config("spark.python.worker.memory","1G") # each worker use 1G to excute
                .config("spark.driver.maxResultSize","3G") #Maximum size of result is 3G
                .config("spark.kryoserializer.buffer.max","1024M")
                 .getOrCreate())
        
        print('Spark session initialized successfully')
        return spark

    except Exception as e:
        logger.error(f"Spark session initialization failed. Error: {e}")
        return None
def load_stream_data():
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
        return df_raw
        write_to_bucket(df_raw)
    else:
        print('No data received!')
        return
    
def get_dataframe(spark, bucket_license, data_path): 
    #config the credential to identify the google cloud hadoop file 
    spark.conf.set("google.cloud.auth.service.account.json.keyfile",bucket_license)
    spark._jsc.hadoopConfiguration().set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')
    spark._jsc.hadoopConfiguration().set('fs.gs.auth.service.account.enable', 'true')
    
    # Read the CSV file into a PySpark DataFrame, drop duplicates, and reset the index
    df_spark = (
        spark.read
        .option("header", "true")  # Assuming the CSV file has a header
        .csv(data_path)
        .dropDuplicates()
        .na.drop()  # Drop rows with any null values
    )
    df_spark = transform_data(df_spark)
    return df_spark

def transform_data(df_spark):
    """
    Transform the initial dataframe to get the final structure.
    
    :param df: Initial dataframe with raw data.
    :return: Transformed dataframe.
    """
    col_floats = ['value', 'price', 'price_in_usd']
    col_ints = ['log_index', 'block_number']
    for x in col_floats:
        df_spark = df_spark.withColumn(x, col(x).cast('float'))

    for x in col_ints:
        df_spark = df_spark.withColumn(x, col(x).cast('int'))
    # df_spark.select(*(col(c).cast("float").alias(c) for c in col_floats))
    df_spark = df_spark.withColumn('Time', col('item_timestamp').cast(TimestampType()))

    # df_spark.printSchema()
    # Show the resulting DataFrame
    # df_spark.show()
    
    return df_spark
    
if __name__ ==  '__main__':
    data_path=f"gs://it4043e-it5384/it4043e/it4043e_group16_problem5/final_etherium_token_transfer.csv"
    bucket_license = "/opt/bucket_connector/lucky-wall-393304-3fbad5f3943c.json"
    spark = initialize_spark_session("Group16_", "spark://34.142.194.212:7077")
    df_spark = get_dataframe(spark, bucket_license, data_path)
    
    # Show the resulting DataFrame
    df_spark.show()
    