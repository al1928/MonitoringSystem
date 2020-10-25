import requests
from pyspark.sql.session import SparkSession

def getMetricData(metric:str):
    # возвращает list метрики из prometheus по необходимой метрике
    response = requests.get('http://localhost:9090/api/v1/query?query=' + metric + '[3m]')
    listData = response.json()['data']['result']
    return listData

def createDictMetric(listData:list):
    # по листу метрики из json данных возвращает словарь процессов
    resourceTypeDict = {}
    for el in listData:
        resourceTypeDict[el["metric"]["resource_type"]] = el["values"]
    return resourceTypeDict

def getDfResourceMetric(resource_type:str, resourceTypeDict: dict):
    # возвращает spark dataframe по заданному процессу
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame(resourceTypeDict[resource_type]).toDF("time", "values")
    return df

def getSparkSession():
    # в идеале должен подключаться к открытой сессии спарка или при её отсутствии открывать новую
    session = SparkSession.builder.config().getOrCreate()
    return session

# example
metric = "ProcessesMemory_private"
dictMetric = createDictMetric(getMetricData(metric))
df = getDfResourceMetric("NVDisplay.Container.exe_2336_1603520705.0_ private", dictMetric)
df.show()
