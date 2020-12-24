import json
import sys
from math import sqrt

import os.path
import create_df_scheme

from pyspark.sql.types import StructType

import HistoricalKMeansModel
from time import ctime
from kafka import KafkaConsumer
from pyspark import SparkContext
from pyspark.ml.clustering import KMeansModel, SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.context import SQLContext


from pyspark.sql.functions import from_json
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler


spark = SparkSession.builder \
        .master("local") \
        .appName("Classification") \
        .config("spark.debug.maxToStringFields", "1000") \
        .getOrCreate()

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
    model = kmeans.fit(df_kmeans)
    print('a5')
    centers = model.clusterCenters()
    print("Cluster Centers: ")
    for center in centers:
        print(center)

    return centers


# shema = create_df_scheme.getShema()
# json_file = 'brutal.json'
# # hist_df = DataFrame
# cluster_centers = list()

from pyspark import SparkConf, SparkContext

def get_centers(spark, model_Kmeans='3metrics.model'):
    # conf = SparkConf().setAppName("Classification")
    # sc = SparkContext(conf=spark)
    model = KMeansModel.load(model_Kmeans)
    centers = model.clusterCenters()
    return centers


def transform_row(data=None):
    # data = {'timestamp': 1608757926.664, 'AGMService.exe_CPU_persent': 0.0, 'AGMService.exe_rss': 4165632.0, 'AGMService.exe_vms': 3747840.0, 'AGSService.exe_CPU_persent': 0.0, 'AGSService.exe_rss': 10645504.0, 'AGSService.exe_vms': 6483968.0, 'AcroRd32.exe_CPU_persent': 0.0, 'AcroRd32.exe_rss': 54312960.0, 'AcroRd32.exe_vms': 182194176.0, 'AdminService.exe_CPU_persent': 0.0, 'AdminService.exe_rss': 4620288.0, 'AdminService.exe_vms': 29597696.0, 'AdobeNotificationClient.exe_CPU_persent': 0.0, 'AdobeNotificationClient.exe_rss': 806912.0, 'AdobeNotificationClient.exe_vms': 7446528.0, 'ApplicationFrameHost.exe_CPU_persent': 0.0, 'ApplicationFrameHost.exe_rss': 23306240.0, 'ApplicationFrameHost.exe_vms': 10862592.0, 'CompPkgSrv.exe_CPU_persent': 0.0, 'CompPkgSrv.exe_rss': 7430144.0, 'CompPkgSrv.exe_vms': 1843200.0, 'DailyNews.exe_CPU_persent': 0.0, 'DailyNews.exe_rss': 24662016.0, 'DailyNews.exe_vms': 8552448.0, 'GoogleCrashHandler.exe_CPU_persent': 0.0, 'GoogleCrashHandler.exe_rss': 180224.0, 'GoogleCrashHandler.exe_vms': 1798144.0, 'GoogleCrashHandler64.exe_CPU_persent': 0.0, 'GoogleCrashHandler64.exe_rss': 143360.0, 'GoogleCrashHandler64.exe_vms': 1781760.0, 'HWDeviceService64.exe_CPU_persent': 0.0, 'HWDeviceService64.exe_rss': 4550656.0, 'HWDeviceService64.exe_vms': 2203648.0, 'IntelCpHDCPSvc.exe_CPU_persent': 0.0, 'IntelCpHDCPSvc.exe_rss': 3895296.0, 'IntelCpHDCPSvc.exe_vms': 1589248.0, 'IntelCpHeciSvc.exe_CPU_persent': 0.0, 'IntelCpHeciSvc.exe_rss': 3907584.0, 'IntelCpHeciSvc.exe_vms': 1695744.0, 'Lenovo.Modern.ImController.PluginHost.CompanionApp.exe_CPU_persent': 0.0, 'Lenovo.Modern.ImController.PluginHost.CompanionApp.exe_rss': 31371264.0, 'Lenovo.Modern.ImController.PluginHost.CompanionApp.exe_vms': 32591872.0, 'Lenovo.Modern.ImController.PluginHost.SettingsApp.exe_CPU_persent': 0.0, 'Lenovo.Modern.ImController.PluginHost.SettingsApp.exe_rss': 39583744.0, 'Lenovo.Modern.ImController.PluginHost.SettingsApp.exe_vms': 39182336.0, 'Lenovo.Modern.ImController.exe_CPU_persent': 0.0, 'Lenovo.Modern.ImController.exe_rss': 126844928.0, 'Lenovo.Modern.ImController.exe_vms': 183713792.0, 'LsaIso.exe_CPU_persent': 0.0, 'LsaIso.exe_rss': 2637824.0, 'LsaIso.exe_vms': 1363968.0, 'MemCompression_CPU_persent': 0.0, 'MemCompression_rss': 1228816384.0, 'MemCompression_vms': 3510272.0, 'Microsoft.Photos.exe_CPU_persent': 0.0, 'Microsoft.Photos.exe_rss': 39813120.0, 'Microsoft.Photos.exe_vms': 54190080.0, 'NVDisplay.Container.exe_CPU_persent': 0.0, 'NVDisplay.Container.exe_rss': 20901888.0, 'NVDisplay.Container.exe_vms': 25989120.0, 'ONENOTEM.EXE_CPU_persent': 0.0, 'ONENOTEM.EXE_rss': 1859584.0, 'ONENOTEM.EXE_vms': 1581056.0, 'OUTLOOK.EXE_CPU_persent': 0.0, 'OUTLOOK.EXE_rss': 98484224.0, 'OUTLOOK.EXE_vms': 81403904.0, 'QHActiveDefense.exe_CPU_persent': 0.0, 'QHActiveDefense.exe_rss': 131162112.0, 'QHActiveDefense.exe_vms': 288182272.0, 'QHSafeTray.exe_CPU_persent': 0.0, 'QHSafeTray.exe_rss': 45596672.0, 'QHSafeTray.exe_vms': 30527488.0, 'QHWatchdog.exe_CPU_persent': 0.0, 'QHWatchdog.exe_rss': 2068480.0, 'QHWatchdog.exe_vms': 1118208.0, 'RAVBg64.exe_CPU_persent': 0.0, 'RAVBg64.exe_rss': 12881920.0, 'RAVBg64.exe_vms': 6373376.0, 'RAVCpl64.exe_CPU_persent': 0.0, 'RAVCpl64.exe_rss': 1384448.0, 'RAVCpl64.exe_vms': 4878336.0, 'RdrCEF.exe_CPU_persent': 0.0, 'RdrCEF.exe_rss': 34127872.0, 'RdrCEF.exe_vms': 58990592.0, 'Registry_CPU_persent': 0.0, 'Registry_rss': 77402112.0, 'Registry_vms': 14589952.0, 'RtkAudioService64.exe_CPU_persent': 0.0, 'RtkAudioService64.exe_rss': 4550656.0, 'RtkAudioService64.exe_vms': 2105344.0, 'RuntimeBroker.exe_CPU_persent': 0.0, 'RuntimeBroker.exe_rss': 23359488.0, 'RuntimeBroker.exe_vms': 7061504.0, 'SETEVENT.exe_CPU_persent': 0.0, 'SETEVENT.exe_rss': 2478080.0, 'SETEVENT.exe_vms': 1445888.0, 'SearchIndexer.exe_CPU_persent': 0.0, 'SearchIndexer.exe_rss': 48455680.0, 'SearchIndexer.exe_vms': 73048064.0, 'SearchUI.exe_CPU_persent': 0.0, 'SearchUI.exe_rss': 75497472.0, 'SearchUI.exe_vms': 119562240.0, 'SecurityHealthService.exe_CPU_persent': 0.0, 'SecurityHealthService.exe_rss': 8171520.0, 'SecurityHealthService.exe_vms': 4079616.0, 'SettingSyncHost.exe_CPU_persent': 0.0, 'SettingSyncHost.exe_rss': 4988928.0, 'SettingSyncHost.exe_vms': 21618688.0, 'SgrmBroker.exe_CPU_persent': 0.0, 'SgrmBroker.exe_rss': 6492160.0, 'SgrmBroker.exe_vms': 6475776.0, 'ShellExperienceHost.exe_CPU_persent': 0.0, 'ShellExperienceHost.exe_rss': 58630144.0, 'ShellExperienceHost.exe_vms': 26738688.0, 'Skype.exe_CPU_persent': 0.0, 'Skype.exe_rss': 154320896.0, 'Skype.exe_vms': 290021376.0, 'SppExtComObj.Exe_CPU_persent': 0.0, 'SppExtComObj.Exe_rss': 8343552.0, 'SppExtComObj.Exe_vms': 2461696.0, 'StartMenuExperienceHost.exe_CPU_persent': 0.0, 'StartMenuExperienceHost.exe_rss': 57753600.0, 'StartMenuExperienceHost.exe_vms': 33017856.0, 'SynTPEnh.exe_CPU_persent': 0.0, 'SynTPEnh.exe_rss': 16076800.0, 'SynTPEnh.exe_vms': 6959104.0, 'SynTPEnhService.exe_CPU_persent': 0.0, 'SynTPEnhService.exe_rss': 4833280.0, 'SynTPEnhService.exe_vms': 2523136.0, 'SynTPHelper.exe_CPU_persent': 0.0, 'SynTPHelper.exe_rss': 4902912.0, 'SynTPHelper.exe_vms': 1163264.0, 'System Idle Process_CPU_persent': 249.1, 'System Idle Process_rss': 8192.0, 'System Idle Process_vms': 61440.0, 'SystemSettings.exe_CPU_persent': 0.0, 'SystemSettings.exe_rss': 61440.0, 'SystemSettings.exe_vms': 24055808.0, 'System_CPU_persent': 4.7, 'System_rss': 1273856.0, 'System_vms': 204800.0, 'Taskmgr.exe_CPU_persent': 6.2, 'Taskmgr.exe_rss': 56782848.0, 'Taskmgr.exe_vms': 37892096.0, 'TeamViewer_Service.exe_CPU_persent': 0.0, 'TeamViewer_Service.exe_rss': 6840320.0, 'TeamViewer_Service.exe_vms': 17948672.0, 'Telegram.exe_CPU_persent': 0.0, 'Telegram.exe_rss': 114475008.0, 'Telegram.exe_vms': 142929920.0, 'UcMapi.exe_CPU_persent': 0.0, 'UcMapi.exe_rss': 31670272.0, 'UcMapi.exe_vms': 16588800.0, 'WUDFHost.exe_CPU_persent': 0.0, 'WUDFHost.exe_rss': 3739648.0, 'WUDFHost.exe_vms': 1884160.0, 'WindowsInternal.ComposableShell.Experiences.TextInput.InputApp.exe_CPU_persent': 0.0, 'WindowsInternal.ComposableShell.Experiences.TextInput.InputApp.exe_rss': 36921344.0, 'WindowsInternal.ComposableShell.Experiences.TextInput.InputApp.exe_vms': 11444224.0, 'YourPhone.exe_CPU_persent': 0.0, 'YourPhone.exe_rss': 33222656.0, 'YourPhone.exe_vms': 25972736.0, '_CPU_persent': 0.0, '_rss': 32903168.0, '_vms': 188416.0, 'armsvc.exe_CPU_persent': 0.0, 'armsvc.exe_rss': 2580480.0, 'armsvc.exe_vms': 1929216.0, 'audiodg.exe_CPU_persent': 10.9, 'audiodg.exe_rss': 20652032.0, 'audiodg.exe_vms': 20406272.0, 'cefutil.exe_CPU_persent': 18.8, 'cefutil.exe_rss': 137203712.0, 'cefutil.exe_vms': 112189440.0, 'chrome.exe_CPU_persent': 0.0, 'chrome.exe_rss': 40394752.0, 'chrome.exe_vms': 28553216.0, 'cmd.exe_CPU_persent': 0.0, 'cmd.exe_rss': 4694016.0, 'cmd.exe_vms': 2977792.0, 'com.docker.service_CPU_persent': 0.0, 'com.docker.service_rss': 14503936.0, 'com.docker.service_vms': 49455104.0, 'conhost.exe_CPU_persent': 0.0, 'conhost.exe_rss': 16793600.0, 'conhost.exe_vms': 7573504.0, 'csrss.exe_CPU_persent': 0.0, 'csrss.exe_rss': 3633152.0, 'csrss.exe_vms': 2002944.0, 'ctfmon.exe_CPU_persent': 0.0, 'ctfmon.exe_rss': 14643200.0, 'ctfmon.exe_vms': 4759552.0, 'dasHost.exe_CPU_persent': 0.0, 'dasHost.exe_rss': 3063808.0, 'dasHost.exe_vms': 933888.0, 'dllhost.exe_CPU_persent': 0.0, 'dllhost.exe_rss': 8015872.0, 'dllhost.exe_vms': 2269184.0, 'dwm.exe_CPU_persent': 0.0, 'dwm.exe_rss': 84291584.0, 'dwm.exe_vms': 117854208.0, 'explorer.exe_CPU_persent': 3.1, 'explorer.exe_rss': 168718336.0, 'explorer.exe_vms': 122630144.0, 'fontdrvhost.exe_CPU_persent': 0.0, 'fontdrvhost.exe_rss': 2547712.0, 'fontdrvhost.exe_vms': 2199552.0, 'fsnotifier64.exe_CPU_persent': 0.0, 'fsnotifier64.exe_rss': 1708032.0, 'fsnotifier64.exe_vms': 516096.0, 'hasplms.exe_CPU_persent': 0.0, 'hasplms.exe_rss': 8163328.0, 'hasplms.exe_vms': 18092032.0, 'ijplmsvc.exe_CPU_persent': 0.0, 'ijplmsvc.exe_rss': 2994176.0, 'ijplmsvc.exe_vms': 1581056.0, 'java.exe_CPU_persent': 0.0, 'java.exe_rss': 425439232.0, 'java.exe_vms': 1280180224.0, 'lsass.exe_CPU_persent': 0.0, 'lsass.exe_rss': 17354752.0, 'lsass.exe_vms': 10788864.0, 'lync.exe_CPU_persent': 0.0, 'lync.exe_rss': 129167360.0, 'lync.exe_vms': 211271680.0, 'mysqld.exe_CPU_persent': 0.0, 'mysqld.exe_rss': 7196672.0, 'mysqld.exe_vms': 345964544.0, 'powershell.exe_CPU_persent': 0.0, 'powershell.exe_rss': 81616896.0, 'powershell.exe_vms': 70729728.0, 'prometheus.exe_CPU_persent': 0.0, 'prometheus.exe_rss': 79675392.0, 'prometheus.exe_vms': 67805184.0, 'pycharm64.exe_CPU_persent': 0.0, 'pycharm64.exe_rss': 978968576.0, 'pycharm64.exe_vms': 1430654976.0, 'python.exe_CPU_persent': 0.0, 'python.exe_rss': 23547904.0, 'python.exe_vms': 14716928.0, 'services.exe_CPU_persent': 0.0, 'services.exe_rss': 9957376.0, 'services.exe_vms': 7335936.0, 'sihost.exe_CPU_persent': 0.0, 'sihost.exe_rss': 29814784.0, 'sihost.exe_vms': 9347072.0, 'smss.exe_CPU_persent': 0.0, 'smss.exe_rss': 1040384.0, 'smss.exe_vms': 1191936.0, 'spoolsv.exe_CPU_persent': 0.0, 'spoolsv.exe_rss': 7114752.0, 'spoolsv.exe_vms': 7917568.0, 'sppsvc.exe_CPU_persent': 0.0, 'sppsvc.exe_rss': 12427264.0, 'sppsvc.exe_vms': 5324800.0, 'sqlwriter.exe_CPU_persent': 0.0, 'sqlwriter.exe_rss': 4403200.0, 'sqlwriter.exe_vms': 2170880.0, 'svchost.exe_CPU_persent': 0.0, 'svchost.exe_rss': 12001280.0, 'svchost.exe_vms': 9502720.0, 'taskhostw.exe_CPU_persent': 0.0, 'taskhostw.exe_rss': 15118336.0, 'taskhostw.exe_vms': 7073792.0, 'unsecapp.exe_CPU_persent': 0.0, 'unsecapp.exe_rss': 4526080.0, 'unsecapp.exe_vms': 1626112.0, 'vmcompute.exe_CPU_persent': 0.0, 'vmcompute.exe_rss': 6520832.0, 'vmcompute.exe_vms': 2478080.0, 'wininit.exe_CPU_persent': 0.0, 'wininit.exe_rss': 4489216.0, 'wininit.exe_vms': 1986560.0, 'winlogon.exe_CPU_persent': 0.0, 'winlogon.exe_rss': 11354112.0, 'winlogon.exe_vms': 2940928.0}

    print(f"### Кол-во метрик строки из стриминга {len(data.keys())}")
    # load schema from json file  -> <class 'pyspark.sql.types.StructType'>
    with open("schema_3metrics.json") as f:
        new_schema = StructType.fromJson(json.load(f))
    df_after = spark.sparkContext.parallelize([]).toDF(new_schema)
    print('### Кол-во метрик в историчных данных: ', len(df_after.columns))
    for k in df_after.columns:
        if k not in data.keys():
            data[k] = 0.0
    df_row = spark.sparkContext.parallelize([data]).toDF(new_schema)
    print(f"### Датафрейм по схеме историчных данных размером {HistoricalKMeansModel.size_df(df_row)}")
    df_row.show()
    df_rename = HistoricalKMeansModel.del_points(df_row)
    df_for_vec = HistoricalKMeansModel.del_time(df_rename)
    return data.values()


# transform_row()

def euclidean(v1, v2):
    return sum((p-q)**2 for p, q in zip(v1, v2)) ** .5

def handler(message):
    records = message.collect()
    centers = get_centers(spark)# сбор rdd в список
    for record in records:
        try:
            # global json_file, shema, cluster_centers
            # if os.path.exists(json_file):
            #     os.remove(json_file)
            # brutal = open(json_file, 'w')
            value = eval(record[1])
            # brutal.write(str(value))
            # brutal.close()

            print(ctime(value['timestamp']))
            print(value)
            row = transform_row(value)
            cost = []
            for center in centers:
                cost.append(euclidean(row, center))
            print(cost)
            print(f"Точка принадлежит класстеру {cost.index(max(cost))+1}")
            # spark = SparkSession.builder.getOrCreate()
            #
            # df = spark.read.schema(shema).json(json_file)
            # df.show(n=1)
            # !!!!!!!!!!  Сюда или куда нибудь ниже надо добавить рассчет расстояни до центров кластеров и вывод.
            #
        except Exception as e:
            print('Получили пустой timestamp.')


def reading_kafka_spark(port: str, topic: str):
    # чтение топика кафки чпо порту через спарк
    sc = spark.sparkContext
    ssc = StreamingContext(sc, 15)
    sc.setLogLevel("WARN")
    sc.setLogLevel("ERROR")

    # Импорт исторических данных
    # global cluster_centers
    # spark = SparkSession.builder.getOrCreate()
    # hist_df = spark.read.schema(shema).json('query3state_16min.json')
    # Нахождение центров кластеров (можно расхардкодить 3, но тогда будет работать оч долго)
    # Сейчас не работает строчка ниже
    # cluster_centers = buildKMeans(hist_df, 3) #
    # print(f'Центры кластеров: {cluster_centers}')

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
topic = 'test'

# print(get_centers())
reading_kafka_spark(port, topic)

"""далее код по классификации"""


# point = df_kmeans.columns
# print(point)
# for center in centers:
#     dis = sqrt(sum([x ** 2 for x in (point - center)]))
#     cost_dist.append(dis)
# print(cost_dist)
