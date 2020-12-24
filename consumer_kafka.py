import json
from time import ctime

from kafka import KafkaConsumer
import sys

import os.path
import create_df_scheme


from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.context import SQLContext


from pyspark.sql.functions import from_json
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler

# По хорошему надо бы отсюда импортировать, а не запихивать сюда код, но там стоит запуск что мешает норм работе...
# from HistoricalKMeansModel import buildKMeans
def buildKMeans(spark_df, k: int):
    # возвращает центроиды кластеров
    print('a')
    vecAssembler = VectorAssembler(inputCols=spark_df.columns[:-1], outputCol="features")
    print('a1')
    df_kmeans = vecAssembler.transform(spark_df).select('timestamp', 'features')
    print('a2')
    # df_kmeans.show()
    print('a3')
    kmeans = KMeans().setK(k).setSeed(1).setFeaturesCol("features")
    print('a4')
    # КРашится тут
    model = kmeans.fit(df_kmeans) # FIXME
    print('a5')
    centers = model.clusterCenters()
    print("Cluster Centers: ")
    for center in centers:
        print(center)

    return centers


shema = create_df_scheme.getShema()
json_file = 'brutal.json'
# hist_df = DataFrame
cluster_centers = list()


def handler(message):
    records = message.collect()  # сбор rdd в список
    for record in records:
        try:
            global json_file, shema, cluster_centers
            if os.path.exists(json_file):
                os.remove(json_file)
            brutal = open(json_file, 'w')
            value = eval(record[1])
            brutal.write(str(value))
            brutal.close()

            print(ctime(value['timestamp']))

            spark = SparkSession.builder.getOrCreate()

            df = spark.read.schema(shema).json(json_file)
            df.show(n=1)
            # !!!!!!!!!!  Сюда или куда нибудь ниже надо добавить рассчет расстояни до центров кластеров и вывод.
            # FIXME
        except Exception as e:
            print('Получили пустой timestamp.')


def reading_kafka_spark(port: str, topic: str):
    # чтение топика кафки чпо порту через спарк

    sc = SparkContext(appName="PythonSparkStreamingKafka")
    ssc = StreamingContext(sc, 15)
    sc.setLogLevel("WARN")
    sc.setLogLevel("ERROR")

    # Импорт исторических данных
    global cluster_centers
    spark = SparkSession.builder.getOrCreate()
    hist_df = spark.read.schema(shema).json('query3state_16min.json')
    # Нахождение центров кластеров (можно расхардкодить 3, но тогда будет работать оч долго)
    # Сейчас не работает строчка ниже
    cluster_centers = buildKMeans(hist_df, 3) # FIXME
    print(f'Центры кластеров: {cluster_centers}')

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