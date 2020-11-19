# from pykafka import KafkaClient
# import json
# from datetime import datetime
# import uuid
# import time
#
# #READ COORDINATES FROM GEOJSON
# input_file = open('./data/bus1.json')
# json_array = json.load(input_file)
# coordinates = json_array['features'][0]['geometry']['coordinates']
#
# #GENERATE UUID
# def generate_uuid():
#     return uuid.uuid4()
#
# #KAFKA PRODUCER
# client = KafkaClient(hosts="localhost:9092")
# topic = client.topics['geodata_final123']
# producer = topic.get_sync_producer()
#
# #CONSTRUCT MESSAGE AND SEND IT TO KAFKA
# data = {}
# data['busline'] = '00001'
#
# def generate_checkpoint(coordinates):
#     i = 0
#     while i < len(coordinates):
#         data['key'] = data['busline'] + '_' + str(generate_uuid())
#         data['timestamp'] = str(datetime.utcnow())
#         data['latitude'] = coordinates[i][1]
#         data['longitude'] = coordinates[i][0]
#         message = json.dumps(data)
#         print(message)
#         producer.produce(message.encode('ascii'))
#         time.sleep(1)
#
#         #if bus reaches last coordinate, start from beginning
#         if i == len(coordinates)-1:
#             i = 0
#         else:
#             i += 1
#
# generate_checkpoint(coordinates)


# from kafka import KafkaProducer
# from kafka.errors import KafkaError
# from pykafka import
#
# producer = KafkaProducer(bootstrap_servers=['broker1:2181'])
#
# # Asynchronous by default
# future = producer.send('my-topic', b'raw_bytes')
#
# # Block for 'synchronous' sends
# try:
#     record_metadata = future.get(timeout=10)
# except KafkaError:
#     # Decide what to do if produce request failed...
#     # log.exception()
#     pass
#
# # Successful result returns assigned partition and offset
# print (record_metadata.topic)
# print (record_metadata.partition)
# print (record_metadata.offset)
#
# # produce keyed messages to enable hashed partitioning
# producer.send('my-topic', key=b'foo', value=b'bar')
#
# # encode objects via msgpack
# producer = KafkaProducer(value_serializer=msgpack.dumps)
# producer.send('msgpack-topic', {'key': 'value'})
#
# # produce json messages
# producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('ascii'))
# producer.send('json-topic', {'key': 'value'})
#
# # produce asynchronously
# for _ in range(100):
#     producer.send('my-topic', b'msg')
#
# def on_send_success(record_metadata):
#     print(record_metadata.topic)
#     print(record_metadata.partition)
#     print(record_metadata.offset)
#
# def on_send_error(excp):
#     log.error('I am an errback', exc_info=excp)
#     # handle exception
#
# # produce asynchronously with callbacks
# producer.send('my-topic', b'raw_bytes').add_callback(on_send_success).add_errback(on_send_error)
#
# # block until all async messages are sent
# producer.flush()
#
# # configure multiple retries
# producer = KafkaProducer(retries=5)

from kafka import KafkaProducer, KafkaConsumer
producer = KafkaProducer(bootstrap_servers='localhost:2181')
for _ in range(100):
    producer.send('foobar', b'some_message_bytes')
#
# consumer = KafkaConsumer('my_favorite_topic')
# for msg in consumer:
#     print (msg)

# from kafka import KafkaProducer, KafkaClient
# #, SimpleClient
# import json
# import yaml
#
# kafka = KafkaClient('localhost:9092')
# # kafka = SimpleClient('localhost:9092')
# producer = KafkaProducer(kafka)
# # jd = json.dumps(d)
#
# with open('MonitoringSystemData.json') as f:
#     for line in f:
#         d = yaml.safe_load(line)
#         jd = json.dumps(d)
#         producer.send_messages(jd, b'zeus_metrics')
#

# from time import sleep
# from json import dumps
# from kafka import KafkaProducer
#
# producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
#                          value_serializer=lambda x:
#                          dumps(x).encode('utf-8'))
# for e in range(1000):
#     data = {'number': e}
#     producer.send('test', value=data)
#     sleep(5)

# from pyspark import SparkConf, SparkContext
# from operator import add
# import sys
# from pyspark.streaming import StreamingContext
# from pyspark.streaming.dstream import KafkaUtils
# import json
# from kafka import SimpleProducer, KafkaClient
# from kafka import KafkaProducer
#
# producer = KafkaProducer(bootstrap_servers='localhost:9092')
#
# def handler(message):
#     records = message.collect()
#     for record in records:
#         producer.send('spark.out', str(record))
#         producer.flush()
#
# def main():
#     sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
#     ssc = StreamingContext(sc, 10)
#
#     brokers, topic = sys.argv[1:]
#     kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
#     kvs.foreachRDD(handler)
#
#     ssc.start()
#     ssc.awaitTermination()
#
# if __name__ == "__main__":
#
#    main()
