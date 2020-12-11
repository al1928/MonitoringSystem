from __future__ import print_function

import sys
import os
import shutil

import findspark
findspark.init()

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession
from pyspark.streaming.kafka import KafkaUtils
import json

outputPath = '/tmp/spark/checkpoint'


# -------------------------------------------------
# Lazily instantiated global instance of SparkSession
# -------------------------------------------------
def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']


# -------------------------------------------------
# What I want to do per each RDD...
# -------------------------------------------------
def process(time, rdd):
    print("===========-----> %s <-----===========" % str(time))

    try:
        spark = getSparkSessionInstance(rdd.context.getConf())

        rowRdd = rdd.map()
        # rowRdd = rdd.map(lambda w: Row(branch=w['branch'],
        #                                currency=w['currency'],
        #                                amount=w['amount']))

        testDataFrame = spark.createDataFrame(rowRdd)

        testDataFrame.createOrReplaceTempView("treasury_stream")

        sql_query = get_sql_query()
        testResultDataFrame = spark.sql(sql_query)
        testResultDataFrame.show(n=5)

        # # Insert into DB
        # try:
        #     testResultDataFrame.write \
        #         .format("jdbc") \
        #         .mode("append") \
        #         .option("driver", 'org.postgresql.Driver') \
        #         .option("url", "jdbc:postgresql://myhabrtest.ciny8bykwxeg.us-east-1.rds.amazonaws.com:5432/habrDB") \
        #         .option("dbtable", "transaction_flow") \
        #         .option("user", "habr") \
        #         .option("password", "habr12345") \
        #         .save()
        #
        # except Exception as e:
        #     print("--> Opps! It seems an Errrorrr with DB working!", e)

    except Exception as e:
        print("--> Opps! Is seems an Error!!!", e)


# -------------------------------------------------
# General function
# -------------------------------------------------
def createContext():
    sc = SparkContext(appName="PythonStreamingKafkaTransaction")
    sc.setLogLevel("ERROR")

    ssc = StreamingContext(sc, 2)

    broker_list, topic = sys.argv[1:]

    try:
        directKafkaStream = KafkaUtils.createDirectStream(ssc,
                                                          [topic],
                                                          {"metadata.broker.list": broker_list})
    except:
        raise ConnectionError("Kafka error: Connection refused: \
                            broker_list={} topic={}".format(broker_list, topic))

    parsed_lines = directKafkaStream.map(lambda v: json.loads(v[1]))
    print(parsed_lines)

    # RDD handling
    # parsed_lines.foreachRDD(process)

    return ssc


# -------------------------------------------------
# Begin
# -------------------------------------------------
if __name__ == "__main__":

    if len(sys.argv) != 3:
        print("Usage: spark_job.py <zk> <topic>", file=sys.stderr)
        exit(-1)

    print("--> Creating new context")
    # if os.path.exists(outputPath):
    #     shutil.rmtree('outputPath')
    try:
        os.mkdir(outputPath, mode=0o777, dir_fd=None)
    except FileExistsError:
        i = 1
        while True:
            try:
                os.mkdir(outputPath + str(i), mode=0o777, dir_fd=None)
                break
            except FileExistsError:
                i += 1

    ssc = StreamingContext.getOrCreate(outputPath, lambda: createContext())
    ssc.start()
    ssc.awaitTermination()

# # Launch
# путь до JAR файла можете сами настроить
# spark-submit --jars spark-streaming-kafka-0-8-assembly_2.11-2.4.7.jar spark_streamer.py localhost:9092 transaction

