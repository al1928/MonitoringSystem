from random import randint

import requests
from json import dumps
from time import ctime, sleep
from kafka import KafkaProducer


#  алгоритм перед запуском файла внизу

def get_string_data(prometheus_metric='PIDMemory'):
    # по названию метрики из прометеуса возвращает json данных (в виде списка)
    response = requests.get(f'http://localhost:9090/api/v1/query?query={prometheus_metric}[15s]')
    json_data = response.json()
    list_data = json_data['data']['result']
    dict_timestamp = dict()

    try:
        dict_timestamp['timestamp'] = list_data[0]['values'][0][0]
        for metric in list_data:
            for t in metric['values']:
                dict_timestamp[metric['metric']['resource_type']] = float(t[1])
        # print(list_data)
        print(ctime(dict_timestamp['timestamp']))
    except Exception as e:
        print('Получили пустой timestamp.')
    return dict_timestamp


my_topic = 'test'
bootstrap_servers = ['localhost:9092']
producer = KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=lambda x: dumps(x).encode('utf-8'))


def send_to_topic(count_message=20):
    while True:
    # for mes in range(count_message):
        data = get_string_data()
    #     data = randint(-10, 10)
        try:
            future = producer.send(topic=my_topic, value=data)
            record_metadata = future.get(timeout=15)

            print('--> The message has been sent to a topic: time: {}\
                    {}, partition: {}, offset: {}'\
                  .format(data['timestamp'],
                          record_metadata.topic,
                          record_metadata.partition,
                          record_metadata.offset))
            sleep(15)

        except Exception as e:
            print('--> It seems an Error occurred: {}'.format(e))

        finally:
            producer.flush()

send_to_topic()


"""
1. run kafka(for PowerShell from /bin/windows)
consol 1
./zookeeper-server-start.bat ../../config/zookeeper.properties
consol 2
./kafka-server-start.bat ../../config/server.properties
consol 3
command to start kafka-topic
./kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic transaction

2. run prometheus

3. run this file
"""
