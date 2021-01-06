from pyspark.sql import *
from pyspark import SparkContext, SparkConf
import json
import numpy as np
import requests
from pyspark.sql import dataframe
from pyspark.sql.types import *
""""""

# "привет мир!"
spark = SparkSession.builder \
    .master("local") \
    .appName("HistData") \
    .config("spark.debug.maxToStringFields", "1000") \
    .getOrCreate()

spark.conf.set("spark.sql.debug.maxToStringFields", "1000")
def getResourceDict (json):
    # по листу метрики из json данных возвращает словарь процессов
    list_data = json['data']['result'] # список словарей по процессам
    # предобработка
    for i in list_data:
        for j in range(len(i["values"])):
            i["values"][j][1] = float(i["values"][j][1])
    values_list = list(map(lambda x: x["values"], list_data))
    resourceTypeDict = dict(map(lambda x: (x["metric"]["resource_type"], x["values"]), list_data))
    # iterObj = iter(resourceTypeDict)
    # print(list(iterObj))
    return resourceTypeDict

def createDF (spark, resourceTypeDict, dictKey):
    return spark.createDataFrame(resourceTypeDict[dictKey]).toDF('Time', dictKey)


# response = requests.get('http://localhost:9090/api/v1/query?query=' + 'PIDMemory' + '[24h]')
# listData = response.json()['data']['result']
with open('query.json', 'r') as j:
    json_data = json.load(j)
    # print(json_data)

resourceTypeDict = getResourceDict(json_data)
column_list = list(resourceTypeDict.keys())
print(len(column_list))
data = list(resourceTypeDict.values())
col = ['Time'] + list(resourceTypeDict.keys())

df = None
for el in column_list[:6]:
    df_schema = StructType([StructField("Time", FloatType(), True), StructField(el, DoubleType(), True)])
    if df is None:
        df = spark.createDataFrame(resourceTypeDict[el])
        df = df.toDF("Time", el)
    else:
        df_right = spark.createDataFrame(resourceTypeDict[el]).toDF("Time", el)
        # df_right[el] = df_right[el].withColumn(el, df_right[el].cast('int').alias(el))
        # df_right[el] = df_right[el].cast(FloatType())
        df = df.join(df_right, "Time", "right")

print(f"#### Columns: {len(df.columns)} #### {df.count()}")
df.printSchema()
df.show()
# data = [[[1604762001.663,"7483392"],[1604762016.664,"7483392"],[1604762031.664,"7483392"],[1604762076.664,"7483392"],[1604762091.663,"7483392"],[1604762106.663,"7483392"],[1604762121.664,"7483392"],[1604762136.664,"7483392"],[1604762151.663,"7483392"],[1604762166.663,"7483392"],[1604762181.664,"7483392"],[1604762196.662,"7483392"],[1604762211.662,"7471104"],[1604762226.663,"7471104"],[1604762241.663,"7471104"],[1604762256.662,"7467008"],[1604762271.662,"7467008"],[1604762286.664,"7454720"],[1604762301.663,"7454720"],[1604762316.663,"7454720"],[1604762331.663,"7446528"],[1604762346.663,"7446528"],[1604762361.663,"7446528"],[1604762376.663,"7446528"],[1604762391.664,"7446528"],[1604762406.664,"7446528"],[1604762421.664,"7446528"],[1604762436.662,"7446528"],[1604762451.662,"7438336"],[1604762466.663,"7438336"],[1604762481.663,"7438336"],[1604762496.662,"7438336"],[1604762511.663,"7438336"],[1604762526.662,"7327744"],[1604762541.663,"7327744"],[1604762556.664,"7327744"],[1604762571.662,"7327744"],[1604762586.663,"7327744"],[1604762601.664,"7315456"],[1604762616.663,"7315456"],[1604762631.663,"7315456"],[1604762646.663,"7315456"],[1604762661.663,"7315456"],[1604762676.664,"7315456"],[1604762691.662,"7307264"],[1604762706.663,"7307264"],[1604762721.664,"7307264"],[1604762736.664,"7307264"],[1604762751.663,"7307264"],[1604762766.662,"7307264"]], [[1604762001.663,"3756032"],[1604762016.664,"3756032"],[1604762031.664,"3756032"],[1604762076.664,"3756032"],[1604762091.663,"3756032"],[1604762106.663,"3756032"],[1604762121.664,"3756032"],[1604762136.664,"3756032"],[1604762151.663,"3756032"],[1604762166.663,"3756032"],[1604762181.664,"3756032"],[1604762196.662,"3756032"],[1604762211.662,"3756032"],[1604762226.663,"3756032"],[1604762241.663,"3756032"],[1604762256.662,"3756032"],[1604762271.662,"3756032"],[1604762286.664,"3756032"],[1604762301.663,"3756032"],[1604762316.663,"3756032"],[1604762331.663,"3756032"],[1604762346.663,"3756032"],[1604762361.663,"3756032"],[1604762376.663,"3756032"],[1604762391.664,"3756032"],[1604762406.664,"3756032"],[1604762421.664,"3756032"],[1604762436.662,"3756032"],[1604762451.662,"3756032"],[1604762466.663,"3756032"],[1604762481.663,"3756032"],[1604762496.662,"3756032"],[1604762511.663,"3756032"],[1604762526.662,"3756032"],[1604762541.663,"3756032"],[1604762556.664,"3756032"],[1604762571.662,"3756032"],[1604762586.663,"3756032"],[1604762601.664,"3756032"],[1604762616.663,"3756032"],[1604762631.663,"3756032"],[1604762646.663,"3756032"],[1604762661.663,"3756032"],[1604762676.664,"3756032"],[1604762691.662,"3756032"],[1604762706.663,"3756032"],[1604762721.664,"3756032"],[1604762736.664,"3756032"],[1604762751.663,"3756032"],[1604762766.662,"3756032"]]]

# df = spark.createDataFrame(resourceTypeDict[keys[0]])

# df = createDF(spark, resourceTypeDict, keys[0])  #- рабочее создание df

# df_rdd = spark.parallelize().map()
# conf = SparkConf().setAppName("Name").setMaster("master")
# sc = SparkContext.getOrCreate()
# row = data[keys[0]][0][0]
# print(row)

def get_row(resourceTypeDict, key):
    # rdd = sc.parallelize(keys).map(lambda x: (, "a" * x))
    # df_rdd = spark.createDataFrame(rdd)

    pass
