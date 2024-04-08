import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
import pyspark.sql.functions as F
from pyspark.sql import types

from config import ASSETS_TOPIC, CREDENTIALS_FILE, GCS_BUCKET, PROJECT_ID

ASSETS_DATASET_ID = "crypto_currency"
ASSETS_TABLE_ID = "assets_all"

pyspark_version = pyspark.__version__

def read_from_kafka(consume_topic: str):
    df = spark \
        .read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", consume_topic) \
        .option("startingOffsets", "earliest") \
        .load()
    return df


def transform_kafka_json(df, spark_schema):
    df_stream = df \
        .select(F.from_json(F.col("value").cast('STRING'),spark_schema)).alias("assets") \
        .select("assets.from_json(CAST(value AS STRING)).*")

    df_ts = df_stream.withColumn('processing_timestamp', F.current_timestamp())

    return df_ts

def sink_bigquery(df, dataset_id: str, table_id: str, gcs_bucket: str):
    df.write \
        .format("bigquery") \
        .option("table", f"{PROJECT_ID}.{dataset_id}.{table_id}") \
        .option("partitionField", "processing_timestamp") \
        .option("clusteredFields", "id") \
        .option("temporaryGcsBucket", gcs_bucket) \
        .mode("append") \
        .save()
    return df


if __name__ == "__main__":

    conf = SparkConf() \
        .setMaster('local[*]') \
        .setAppName('CryptoCurrencyConsumer') \
        .set("spark.jars", "/opt/homebrew/lib/gcs-connector-hadoop3-2.2.5.jar") \
        .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", CREDENTIALS_FILE) \
        .set("spark.jars.packages", f"org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark_version},com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.15.1-beta")

    sc = SparkContext(conf=conf)

    hadoop_conf = sc._jsc.hadoopConfiguration()

    hadoop_conf.set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", CREDENTIALS_FILE)
    hadoop_conf.set("fs.gs.auth.service.account.enable", "true")

    assets_spark_schema = types.StructType() \
        .add("id", types.StringType()) \
        .add("rank", types.StringType()) \
        .add("symbol", types.StringType()) \
        .add("name", types.StringType()) \
        .add("supply", types.StringType()) \
        .add("maxSupply", types.StringType()) \
        .add("marketCapUsd", types.StringType()) \
        .add("priceUsd", types.StringType()) \
        .add("changePercent24Hr", types.StringType()) \
        .add("vwap24Hr", types.StringType()) \
        .add("timestamp", types.StringType())


    spark = SparkSession \
        .builder \
        .config(conf=sc.getConf()) \
        .getOrCreate()

    spark.conf.set("spark.sql.repl.eagerEval.enabled", True)
    spark.sparkContext.setLogLevel('WARN')

    # read streaming data
    df_consume_stream = read_from_kafka(consume_topic=ASSETS_TOPIC)
    print(df_consume_stream.printSchema())

    # parse data
    df_assets = transform_kafka_json(df_consume_stream, assets_spark_schema)
    print(df_assets.printSchema())

    # write output to Bigquery
    write_bq = sink_bigquery(df_assets, ASSETS_DATASET_ID, ASSETS_TABLE_ID, GCS_BUCKET)

    write_bq.awaitAnyTermination()
