#!/usr/bin/env python3
import os
import json
import requests
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
from confluent_kafka import Producer

def create_spark_session():
    return SparkSession.builder \
        .appName("StockDataProject") \
        .config("spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true") \
        .config("spark.executor.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true") \
        .getOrCreate()

def request_stock_data(api_key, symbol="IBM", interval="30min"):
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={symbol}&interval={interval}&apikey={api_key}'
    response = requests.get(url)
    data = response.json()
    
    time_series_key = f"Time Series ({interval})"
    if time_series_key not in data:
        raise ValueError("Invalid response or API limit exceeded")
    
    records = []
    for timestamp, values in data[time_series_key].items():
        records.append((
            symbol,
            timestamp,
            float(values["1. open"]),
            float(values["2. high"]),
            float(values["3. low"]),
            float(values["4. close"]),
            int(values["5. volume"])
        ))
    return records

def request_report(err, msg):
    if err:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def send_to_kafka(df, bootstrap_servers='kafka:9092', topic='stock_data_topic'):
    producer = Producer({'bootstrap.servers': bootstrap_servers})
    for row in df.collect():
        message = json.dumps({
            "symbol": row["symbol"],
            "timestamp": row["timestamp"],
            "open": row["open"],
            "high": row["high"],
            "low": row["low"],
            "close": row["close"],
            "volume": row["volume"]
        })
        producer.produce(topic, message.encode('utf-8'), callback=request_report)
    producer.flush()
    time.sleep(2)

def write_to_postgres(df, epoch_id):
    df.write.format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/mydatabase") \
        .option("dbtable", "stock_data") \
        .option("user", "postgres") \
        .option("password", "mysecretpassword") \
        .mode("append") \
        .save()

if __name__ == "__main__":
    spark = create_spark_session()
    
    try:
        API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")
        records = request_stock_data(API_KEY)
        columns = ["symbol", "timestamp", "open", "high", "low", "close", "volume"]
        df = spark.createDataFrame(records, columns)
        df.show()
        
        send_to_kafka(df)
        print("Successfully sent data to Kafka")
        
        schema = StructType([
            StructField("symbol", StringType()),
            StructField("timestamp", TimestampType()),
            StructField("open", DoubleType()),
            StructField("high", DoubleType()),
            StructField("low", DoubleType()),
            StructField("close", DoubleType()),
            StructField("volume", IntegerType())
        ])
        
        kafka_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("subscribe", "stock_data_topic") \
            .load() \
            .selectExpr("CAST(value AS STRING)") \
            .select(from_json("value", schema).alias("data")) \
            .select("data.*")
        
        query = kafka_df.writeStream \
            .foreachBatch(write_to_postgres) \
            .start()
        
        # For demonstration, read some data from PostgreSQL
        postgres_df = spark.read \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/mydatabase") \
            .option("dbtable", "stock_data") \
            .option("user", "postgres") \
            .option("password", "mysecretpassword") \
            .load()
        
        postgres_df.show(10)
        
        query.awaitTermination()
        
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        raise
    
    finally:
        spark.stop()