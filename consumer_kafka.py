import json
from time import ctime

from kafka import KafkaConsumer
import sys

import os.path

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.context import SQLContext

import create_df_scheme
from pyspark.sql.functions import from_json


shema = create_df_scheme.getShema()
sc = SparkContext
json_file = 'brutal.json'


def handler(message):
    records = message.collect()  # сбор rdd в список
    for record in records:
        try:
            global json_file
            if os.path.exists(json_file):
                print('aaa')
                os.remove(json_file)
            brutal = open(json_file, 'w')
            value = eval(record[1])
            # print(value)
            # print(record)
            brutal.write(str(value))
            brutal.close()
            # exit(1)
            # DataFrame.
            # print(sc.parallelize([json_file]))
            # value.printSchema()
            # json_df = from_json(value, shema)
            print(ctime(value['timestamp']))
            # print(value)
            # global spark
            global shema
            # global sc
            spark = SparkSession.builder\
                .getOrCreate()

            # df = spark.read.json(sc.parallelize([value]))
            # df.show(truncate=False)

            df = spark.read.schema(shema).json(json_file)
            # df.printSchema()
            df.show(n=1)

            # df = spark.createDataFrame(spark.sparkContext.emptyRDD(), shema)

            # df = spark.createDataFrame(record, shema)

            # print(message.toDF())

            # df = spark.createDataFrame(message, shema)
            # df.printSchema()
            # print(df)
            # df.show(n=1)

            # df = spark.read.json(sc.parallelize([value]))
            # print(df)


            # df = spark.read.json(Seq(jsonStr).toDS)
            # spark.read.json(sc.parallelize([newJson]))
            # df = pyspark.read.json(Seq(jsonStr).toDS)
        except Exception as e:
            print('Получили пустой timestamp.')


def reading_kafka_spark(port: str, topic: str):
    # чтение топика кафки чпо порту через спарк

    sc = SparkContext(appName="PythonSparkStreamingKafka")
    ssc = StreamingContext(sc, 15)
    sc.setLogLevel("WARN")
    sc.setLogLevel("ERROR")
    # brokers, topic = sys.argv[1:]
    kafkaStream = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": port})
    kafkaStream.foreachRDD(handler)
    ssc.start()
    ssc.awaitTermination()


def reading_kafka_without_spark(port: str, topic: str):
    # Производит чтение топика kafka по указанному порту без помощи спарк_стриминга
    # Initialize consumer variable
    consumer = KafkaConsumer(topic, group_id='group1', bootstrap_servers=list(port))
    # Read and print message from consumer
    for msg in consumer:
        print('###')
        print(f'Topic Name = {msg.topic}, Message = {msg.value}')

port = 'localhost:9092'
topic = 'transaction'

reading_kafka_spark(port, topic)

# model = KMeansModel.load('q3state_16min.model')
# centers = model.clusterCenters()
# # k = model.k()
# # n_cluster = model.predict(get_string_data())
# # print(f'Всего кластеров:  \n Точка принадлежит к кластеру №{n_cluster}')
# print("Cluster Centers: ")
# for center in centers:
#     print(len(center))