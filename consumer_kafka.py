import json
from time import ctime

from kafka import KafkaConsumer
import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


def handler(message):
    records = message.collect()  # сбор rdd в список
    for record in records:
        value = eval(record[1])
        print(ctime(value['timestamp']))
        print(value)


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