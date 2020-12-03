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

class KafkaJsonSender:

    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                                      value_serializer=lambda x: dumps(x).encode('utf-8'),
                                      compression_type='gzip')
        self.topic = 'transaction'

    def __del__(self):
        self.producer.flush()

    def send_data(self, transaction_data: dict):
        for item in transaction_data:
            try:
                future = self.producer.send(topic=self.topic, value=item)
                record_metadata = future.get(timeout=10)
                print(item)

                print('--> The message has been sent to a topic: \
                        {}, partition: {}, offset: {}' \
                      .format(record_metadata.topic,
                              record_metadata.partition,
                              record_metadata.offset))
            except Exception as e:
                print('--> It seems an Error occurred: {}'.format(e))


# sender = KafkaJsonSender()
# data = {'timestamp': 1604762226.663, 'AGMService.exe_5208_1604522690.0_0': 7471104.0, 'AGMService.exe_5208_1604522690.0_1': 3756032.0, 'AGMService.exe_5208_1604522690.0_10': 3911680.0, 'AGMService.exe_5208_1604522690.0_11': 3756032.0, 'AGMService.exe_5208_1604522690.0_2': 5587.0, 'AGMService.exe_5208_1604522690.0_3': 11341824.0, 'AGMService.exe_5208_1604522690.0_4': 7471104.0, 'AGMService.exe_5208_1604522690.0_5': 138968.0, 'AGMService.exe_5208_1604522690.0_6': 138968.0, 'AGMService.exe_5208_1604522690.0_7': 15880.0, 'AGMService.exe_5208_1604522690.0_8': 14680.0, 'AGMService.exe_5208_1604522690.0_9': 3756032.0, 'AGSService.exe_5228_1604522690.0_0': 12140544.0, 'AGSService.exe_5228_1604522690.0_1': 5341184.0, 'AGSService.exe_5228_1604522690.0_10': 7024640.0, 'AGSService.exe_5228_1604522690.0_11': 5341184.0, 'AGSService.exe_5228_1604522690.0_2': 40173.0, 'AGSService.exe_5228_1604522690.0_3': 20615168.0, 'AGSService.exe_5228_1604522690.0_4': 12140544.0, 'AGSService.exe_5228_1604522690.0_5': 375496.0, 'AGSService.exe_5228_1604522690.0_6': 184304.0, 'AGSService.exe_5228_1604522690.0_7': 28752.0, 'AGSService.exe_5228_1604522690.0_8': 22696.0, 'AGSService.exe_5228_1604522690.0_9': 5341184.0, 'AcroRd32.exe_17332_1604753701.0_0': 56713216.0, 'AcroRd32.exe_17332_1604753701.0_1': 180985856.0, 'AcroRd32.exe_17332_1604753701.0_10': 196096000.0, 'AcroRd32.exe_17332_1604753701.0_11': 180985856.0, 'AcroRd32.exe_17332_1604753701.0_2': 228362.0, 'AcroRd32.exe_17332_1604753701.0_3': 224301056.0, 'AcroRd32.exe_17332_1604753701.0_4': 56713216.0, 'AcroRd32.exe_17332_1604753701.0_5': 555936.0, 'AcroRd32.exe_17332_1604753701.0_6': 508824.0, 'AcroRd32.exe_17332_1604753701.0_7': 83848.0, 'AcroRd32.exe_17332_1604753701.0_8': 81944.0, 'AcroRd32.exe_17332_1604753701.0_9': 180985856.0, 'AcroRd32.exe_20736_1604753700.0_0': 20430848.0, 'AcroRd32.exe_20736_1604753700.0_1': 13590528.0, 'AcroRd32.exe_20736_1604753700.0_10': 14520320.0, 'AcroRd32.exe_20736_1604753700.0_11': 13590528.0, 'AcroRd32.exe_20736_1604753700.0_2': 19829.0, 'AcroRd32.exe_20736_1604753700.0_3': 38535168.0, 'AcroRd32.exe_20736_1604753700.0_4': 20430848.0, 'AcroRd32.exe_20736_1604753700.0_5': 271904.0, 'AcroRd32.exe_20736_1604753700.0_6': 249216.0, 'AcroRd32.exe_20736_1604753700.0_7': 29104.0, 'AcroRd32.exe_20736_1604753700.0_8': 26096.0, 'AcroRd32.exe_20736_1604753700.0_9': 13590528.0, 'AdminService.exe_4788_1604522690.0_0': 5124096.0, 'AdminService.exe_4788_1604522690.0_1': 17002496.0, 'AdminService.exe_4788_1604522690.0_10': 17002496.0, 'AdminService.exe_4788_1604522690.0_11': 17002496.0, 'AdminService.exe_4788_1604522690.0_2': 7846.0, 'AdminService.exe_4788_1604522690.0_3': 10592256.0, 'AdminService.exe_4788_1604522690.0_4': 5124096.0, 'AdminService.exe_4788_1604522690.0_5': 137136.0, 'AdminService.exe_4788_1604522690.0_6': 137136.0, 'AdminService.exe_4788_1604522690.0_7': 10592.0, 'AdminService.exe_4788_1604522690.0_8': 9712.0, 'AdminService.exe_4788_1604522690.0_9': 17002496.0, 'AdobeNotificationClient.exe_16300_1604747337.0_0': 954368.0, 'AdobeNotificationClient.exe_16300_1604747337.0_1': 7712768.0, 'AdobeNotificationClient.exe_16300_1604747337.0_10': 8855552.0, 'AdobeNotificationClient.exe_16300_1604747337.0_11': 7712768.0, 'AdobeNotificationClient.exe_16300_1604747337.0_2': 8313.0, 'AdobeNotificationClient.exe_16300_1604747337.0_3': 30294016.0, 'AdobeNotificationClient.exe_16300_1604747337.0_4': 954368.0, 'AdobeNotificationClient.exe_16300_1604747337.0_5': 420256.0, 'AdobeNotificationClient.exe_16300_1604747337.0_6': 396416.0, 'AdobeNotificationClient.exe_16300_1604747337.0_7': 21776.0, 'AdobeNotificationClient.exe_16300_1604747337.0_8': 20848.0, 'AdobeNotificationClient.exe_16300_1604747337.0_9': 7712768.0, 'ApplicationFrameHost.exe_9484_1604734219.0_0': 22421504.0, 'ApplicationFrameHost.exe_9484_1604734219.0_1': 12783616.0, 'ApplicationFrameHost.exe_9484_1604734219.0_10': 14344192.0, 'ApplicationFrameHost.exe_9484_1604734219.0_11': 12783616.0, 'ApplicationFrameHost.exe_9484_1604734219.0_2': 10343.0,
#
# 'ApplicationFrameHost.exe_9484_1604734219.0_3': 29806592.0, 'ApplicationFrameHost.exe_9484_1604734219.0_4': 22421504.0, 'ApplicationFrameHost.exe_9484_1604734219.0_5': 373744.0, 'ApplicationFrameHost.exe_9484_1604734219.0_6': 332104.0, 'ApplicationFrameHost.exe_9484_1604734219.0_7': 23248.0, 'ApplicationFrameHost.exe_9484_1604734219.0_8': 21344.0, 'ApplicationFrameHost.exe_9484_1604734219.0_9': 12783616.0, 'Code.exe_11148_1604754626.0_0': 80044032.0, 'Code.exe_11148_1604754626.0_1': 41377792.0, 'Code.exe_11148_1604754626.0_10': 51519488.0, 'Code.exe_11148_1604754626.0_11': 41377792.0, 'Code.exe_11148_1604754626.0_2': 54965.0, 'Code.exe_11148_1604754626.0_3': 100372480.0, 'Code.exe_11148_1604754626.0_4': 80044032.0, 'Code.exe_11148_1604754626.0_5': 1021136.0, 'Code.exe_11148_1604754626.0_6': 790224.0, 'Code.exe_11148_1604754626.0_7': 57608.0, 'Code.exe_11148_1604754626.0_8': 47808.0, 'Code.exe_11148_1604754626.0_9': 41377792.0, 'Code.exe_13984_1604754627.0_0': 12980224.0, 'Code.exe_13984_1604754627.0_1': 9932800.0, 'Code.exe_13984_1604754627.0_10': 10031104.0, 'Code.exe_13984_1604754627.0_11': 9932800.0}, {'timestamp': 1604762241.663, 'AGMService.exe_5208_1604522690.0_0': 7471104.0, 'AGMService.exe_5208_1604522690.0_1': 3756032.0, 'AGMService.exe_5208_1604522690.0_10': 3911680.0, 'AGMService.exe_5208_1604522690.0_11': 3756032.0, 'AGMService.exe_5208_1604522690.0_2': 5587.0, 'AGMService.exe_5208_1604522690.0_3': 11341824.0, 'AGMService.exe_5208_1604522690.0_4': 7471104.0, 'AGMService.exe_5208_1604522690.0_5': 138968.0, 'AGMService.exe_5208_1604522690.0_6': 138968.0, 'AGMService.exe_5208_1604522690.0_7': 15880.0, 'AGMService.exe_5208_1604522690.0_8': 14680.0, 'AGMService.exe_5208_1604522690.0_9': 3756032.0, 'AGSService.exe_5228_1604522690.0_0': 12140544.0, 'AGSService.exe_5228_1604522690.0_1': 5341184.0, 'AGSService.exe_5228_1604522690.0_10': 7024640.0, 'AGSService.exe_5228_1604522690.0_11': 5341184.0, 'AGSService.exe_5228_1604522690.0_2': 40173.0, 'AGSService.exe_5228_1604522690.0_3': 20615168.0, 'AGSService.exe_5228_1604522690.0_4': 12140544.0, 'AGSService.exe_5228_1604522690.0_5': 375496.0, 'AGSService.exe_5228_1604522690.0_6': 184304.0, 'AGSService.exe_5228_1604522690.0_7': 28752.0, 'AGSService.exe_5228_1604522690.0_8': 22696.0, 'AGSService.exe_5228_1604522690.0_9': 5341184.0, 'AcroRd32.exe_17332_1604753701.0_0': 56713216.0, 'AcroRd32.exe_17332_1604753701.0_1': 180985856.0, 'AcroRd32.exe_17332_1604753701.0_10': 196096000.0, 'AcroRd32.exe_17332_1604753701.0_11': 180985856.0, 'AcroRd32.exe_17332_1604753701.0_2': 228362.0, 'AcroRd32.exe_17332_1604753701.0_3': 224301056.0, 'AcroRd32.exe_17332_1604753701.0_4': 56713216.0, 'AcroRd32.exe_17332_1604753701.0_5': 555936.0, 'AcroRd32.exe_17332_1604753701.0_6': 508824.0, 'AcroRd32.exe_17332_1604753701.0_7': 83848.0, 'AcroRd32.exe_17332_1604753701.0_8': 81944.0, 'AcroRd32.exe_17332_1604753701.0_9': 180985856.0, 'AcroRd32.exe_20736_1604753700.0_0': 20430848.0, 'AcroRd32.exe_20736_1604753700.0_1': 13590528.0, 'AcroRd32.exe_20736_1604753700.0_10': 14520320.0, 'AcroRd32.exe_20736_1604753700.0_11': 13590528.0, 'AcroRd32.exe_20736_1604753700.0_2': 19829.0, 'AcroRd32.exe_20736_1604753700.0_3': 38535168.0, 'AcroRd32.exe_20736_1604753700.0_4': 20430848.0, 'AcroRd32.exe_20736_1604753700.0_5': 271904.0, 'AcroRd32.exe_20736_1604753700.0_6': 249216.0, 'AcroRd32.exe_20736_1604753700.0_7': 29104.0, 'AcroRd32.exe_20736_1604753700.0_8': 26096.0, 'AcroRd32.exe_20736_1604753700.0_9': 13590528.0, 'AdminService.exe_4788_1604522690.0_0': 5124096.0, 'AdminService.exe_4788_1604522690.0_1': 17002496.0, 'AdminService.exe_4788_1604522690.0_10': 17002496.0, 'AdminService.exe_4788_1604522690.0_11': 17002496.0, 'AdminService.exe_4788_1604522690.0_2': 7846.0, 'AdminService.exe_4788_1604522690.0_3': 10592256.0, 'AdminService.exe_4788_1604522690.0_4': 5124096.0, 'AdminService.exe_4788_1604522690.0_5': 137136.0, 'AdminService.exe_4788_1604522690.0_6': 137136.0, 'AdminService.exe_4788_1604522690.0_7': 10592.0, 'AdminService.exe_4788_1604522690.0_8': 9712.0, 'AdminService.exe_4788_1604522690.0_9': 17002496.0,
#
# 'AdobeNotificationClient.exe_16300_1604747337.0_0': 954368.0, 'AdobeNotificationClient.exe_16300_1604747337.0_1': 7712768.0, 'AdobeNotificationClient.exe_16300_1604747337.0_10': 8855552.0, 'AdobeNotificationClient.exe_16300_1604747337.0_11': 7712768.0, 'AdobeNotificationClient.exe_16300_1604747337.0_2': 8313.0, 'AdobeNotificationClient.exe_16300_1604747337.0_3': 30294016.0, 'AdobeNotificationClient.exe_16300_1604747337.0_4': 954368.0, 'AdobeNotificationClient.exe_16300_1604747337.0_5': 420256.0, 'AdobeNotificationClient.exe_16300_1604747337.0_6': 396416.0, 'AdobeNotificationClient.exe_16300_1604747337.0_7': 21776.0, 'AdobeNotificationClient.exe_16300_1604747337.0_8': 20848.0, 'AdobeNotificationClient.exe_16300_1604747337.0_9': 7712768.0, 'ApplicationFrameHost.exe_9484_1604734219.0_0': 22421504.0, 'ApplicationFrameHost.exe_9484_1604734219.0_1': 12783616.0, 'ApplicationFrameHost.exe_9484_1604734219.0_10': 14344192.0, 'ApplicationFrameHost.exe_9484_1604734219.0_11': 12783616.0, 'ApplicationFrameHost.exe_9484_1604734219.0_2': 10343.0, 'ApplicationFrameHost.exe_9484_1604734219.0_3': 29806592.0, 'ApplicationFrameHost.exe_9484_1604734219.0_4': 22421504.0, 'ApplicationFrameHost.exe_9484_1604734219.0_5': 373744.0, 'ApplicationFrameHost.exe_9484_1604734219.0_6': 332104.0, 'ApplicationFrameHost.exe_9484_1604734219.0_7': 23248.0, 'ApplicationFrameHost.exe_9484_1604734219.0_8': 21344.0, 'ApplicationFrameHost.exe_9484_1604734219.0_9': 12783616.0, 'Code.exe_11148_1604754626.0_0': 80044032.0, 'Code.exe_11148_1604754626.0_1': 41377792.0, 'Code.exe_11148_1604754626.0_10': 51519488.0, 'Code.exe_11148_1604754626.0_11': 41377792.0, 'Code.exe_11148_1604754626.0_2': 54965.0, 'Code.exe_11148_1604754626.0_3': 100372480.0, 'Code.exe_11148_1604754626.0_4': 80044032.0, 'Code.exe_11148_1604754626.0_5': 1021136.0, 'Code.exe_11148_1604754626.0_6': 790224.0, 'Code.exe_11148_1604754626.0_7': 57608.0, 'Code.exe_11148_1604754626.0_8': 47808.0, 'Code.exe_11148_1604754626.0_9': 41377792.0, 'Code.exe_13984_1604754627.0_0': 12980224.0, 'Code.exe_13984_1604754627.0_1': 9932800.0, 'Code.exe_13984_1604754627.0_10': 10031104.0, 'Code.exe_13984_1604754627.0_11': 9932800.0}, {'timestamp': 1604762256.662, 'AGMService.exe_5208_1604522690.0_0': 7467008.0, 'AGMService.exe_5208_1604522690.0_1': 3756032.0, 'AGMService.exe_5208_1604522690.0_10': 3911680.0, 'AGMService.exe_5208_1604522690.0_11': 3756032.0, 'AGMService.exe_5208_1604522690.0_2': 5587.0, 'AGMService.exe_5208_1604522690.0_3': 11341824.0, 'AGMService.exe_5208_1604522690.0_4': 7467008.0, 'AGMService.exe_5208_1604522690.0_5': 138968.0, 'AGMService.exe_5208_1604522690.0_6': 138968.0, 'AGMService.exe_5208_1604522690.0_7': 15880.0, 'AGMService.exe_5208_1604522690.0_8': 14680.0, 'AGMService.exe_5208_1604522690.0_9': 3756032.0, 'AGSService.exe_5228_1604522690.0_0': 12124160.0, 'AGSService.exe_5228_1604522690.0_1': 5341184.0, 'AGSService.exe_5228_1604522690.0_10': 7024640.0, 'AGSService.exe_5228_1604522690.0_11': 5341184.0, 'AGSService.exe_5228_1604522690.0_2': 40173.0, 'AGSService.exe_5228_1604522690.0_3': 20615168.0, 'AGSService.exe_5228_1604522690.0_4': 12124160.0, 'AGSService.exe_5228_1604522690.0_5': 375496.0, 'AGSService.exe_5228_1604522690.0_6': 184304.0, 'AGSService.exe_5228_1604522690.0_7': 28752.0, 'AGSService.exe_5228_1604522690.0_8': 22696.0, 'AGSService.exe_5228_1604522690.0_9': 5341184.0, 'AcroRd32.exe_17332_1604753701.0_0': 56668160.0, 'AcroRd32.exe_17332_1604753701.0_1': 180985856.0, 'AcroRd32.exe_17332_1604753701.0_10': 196096000.0, 'AcroRd32.exe_17332_1604753701.0_11': 180985856.0, 'AcroRd32.exe_17332_1604753701.0_2': 228362.0, 'AcroRd32.exe_17332_1604753701.0_3': 224301056.0, 'AcroRd32.exe_17332_1604753701.0_4': 56668160.0, 'AcroRd32.exe_17332_1604753701.0_5': 555936.0, 'AcroRd32.exe_17332_1604753701.0_6': 508824.0, 'AcroRd32.exe_17332_1604753701.0_7': 83848.0, 'AcroRd32.exe_17332_1604753701.0_8': 81944.0, 'AcroRd32.exe_17332_1604753701.0_9': 180985856.0, 'AcroRd32.exe_20736_1604753700.0_0': 20418560.0, 'AcroRd32.exe_20736_1604753700.0_1': 13590528.0, 'AcroRd32.exe_20736_1604753700.0_10':
#
# 14520320.0, 'AcroRd32.exe_20736_1604753700.0_11': 13590528.0, 'AcroRd32.exe_20736_1604753700.0_2': 19829.0, 'AcroRd32.exe_20736_1604753700.0_3': 38535168.0, 'AcroRd32.exe_20736_1604753700.0_4': 20418560.0, 'AcroRd32.exe_20736_1604753700.0_5': 271904.0, 'AcroRd32.exe_20736_1604753700.0_6': 249216.0, 'AcroRd32.exe_20736_1604753700.0_7': 29104.0, 'AcroRd32.exe_20736_1604753700.0_8': 26096.0, 'AcroRd32.exe_20736_1604753700.0_9': 13590528.0, 'AdminService.exe_4788_1604522690.0_0': 5124096.0, 'AdminService.exe_4788_1604522690.0_1': 17002496.0, 'AdminService.exe_4788_1604522690.0_10': 17002496.0, 'AdminService.exe_4788_1604522690.0_11': 17002496.0, 'AdminService.exe_4788_1604522690.0_2': 7846.0, 'AdminService.exe_4788_1604522690.0_3': 10592256.0, 'AdminService.exe_4788_1604522690.0_4': 5124096.0, 'AdminService.exe_4788_1604522690.0_5': 137136.0, 'AdminService.exe_4788_1604522690.0_6': 137136.0, 'AdminService.exe_4788_1604522690.0_7': 10592.0, 'AdminService.exe_4788_1604522690.0_8': 9712.0, 'AdminService.exe_4788_1604522690.0_9': 17002496.0, 'AdobeNotificationClient.exe_16300_1604747337.0_0': 954368.0, 'AdobeNotificationClient.exe_16300_1604747337.0_1': 7712768.0, 'AdobeNotificationClient.exe_16300_1604747337.0_10': 8855552.0, 'AdobeNotificationClient.exe_16300_1604747337.0_11': 7712768.0, 'AdobeNotificationClient.exe_16300_1604747337.0_2': 8313.0, 'AdobeNotificationClient.exe_16300_1604747337.0_3': 30294016.0, 'AdobeNotificationClient.exe_16300_1604747337.0_4': 954368.0, 'AdobeNotificationClient.exe_16300_1604747337.0_5': 420256.0, 'AdobeNotificationClient.exe_16300_1604747337.0_6': 396416.0, 'AdobeNotificationClient.exe_16300_1604747337.0_7': 21776.0, 'AdobeNotificationClient.exe_16300_1604747337.0_8': 20848.0, 'AdobeNotificationClient.exe_16300_1604747337.0_9': 7712768.0, 'ApplicationFrameHost.exe_9484_1604734219.0_0': 22409216.0, 'ApplicationFrameHost.exe_9484_1604734219.0_1': 12783616.0, 'ApplicationFrameHost.exe_9484_1604734219.0_10': 14344192.0, 'ApplicationFrameHost.exe_9484_1604734219.0_11': 12783616.0, 'ApplicationFrameHost.exe_9484_1604734219.0_2': 10343.0, 'ApplicationFrameHost.exe_9484_1604734219.0_3': 29806592.0, 'ApplicationFrameHost.exe_9484_1604734219.0_4': 22409216.0, 'ApplicationFrameHost.exe_9484_1604734219.0_5': 373744.0, 'ApplicationFrameHost.exe_9484_1604734219.0_6': 332104.0, 'ApplicationFrameHost.exe_9484_1604734219.0_7': 23248.0, 'ApplicationFrameHost.exe_9484_1604734219.0_8': 21344.0, 'ApplicationFrameHost.exe_9484_1604734219.0_9': 12783616.0, 'Code.exe_11148_1604754626.0_0': 79974400.0, 'Code.exe_11148_1604754626.0_1': 41377792.0, 'Code.exe_11148_1604754626.0_10': 51519488.0, 'Code.exe_11148_1604754626.0_11': 41377792.0, 'Code.exe_11148_1604754626.0_2': 55026.0, 'Code.exe_11148_1604754626.0_3': 100372480.0, 'Code.exe_11148_1604754626.0_4': 79974400.0, 'Code.exe_11148_1604754626.0_5': 1021136.0, 'Code.exe_11148_1604754626.0_6': 790224.0, 'Code.exe_11148_1604754626.0_7': 57608.0, 'Code.exe_11148_1604754626.0_8': 47808.0, 'Code.exe_11148_1604754626.0_9': 41377792.0, 'Code.exe_13984_1604754627.0_0': 12976128.0, 'Code.exe_13984_1604754627.0_1': 9932800.0, 'Code.exe_13984_1604754627.0_10': 10031104.0, 'Code.exe_13984_1604754627.0_11': 9932800.0}
# sender.send_data(data)
