from json import dumps
from numpy.random import choice, randint
from kafka import KafkaProducer


def get_random_value():
    new_dict = {}

    branch_list = ['Kazan', 'SPB', 'Novosibirsk', 'Surgut']
    currency_list = ['RUB', 'USD', 'EUR', 'GBP']

    new_dict['branch'] = choice(branch_list)
    new_dict['currency'] = choice(currency_list)
    new_dict['amount'] = randint(-100, 100)

    return new_dict


producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: dumps(x).encode('utf-8'),
                         compression_type='gzip')
for i in range(10):
    my_topic = 'transaction'
    data = get_random_value()

    try:
        future = producer.send(topic=my_topic, value=data)
        record_metadata = future.get(timeout=10)

        print('--> The message has been sent to a topic: \
                {}, partition: {}, offset: {}' \
              .format(record_metadata.topic,
                      record_metadata.partition,
                      record_metadata.offset))

    except Exception as e:
        print('--> It seems an Error occurred: {}'.format(e))

    finally:
        producer.flush()
