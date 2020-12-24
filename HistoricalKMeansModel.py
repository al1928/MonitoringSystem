import json
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import approx_count_distinct
from pyspark.sql.functions import avg
from pyspark.sql.session import SparkSession


def delPoints(df):  # УБИРАЕТ ТОЧКИ ИЗ НАЗВАНИЙ СТОЛБЦОВ
    tempList = []
    for col in df.columns:
        new_name = col.strip()
        new_name = "".join(new_name.split())
        new_name = new_name.replace('.', '')
        tempList.append(new_name)
    # print(tempList) #Just for the sake of it
    df_spark = df.toDF(*tempList)
    return df_spark


def delTime(df):  # удаление столбца Time
    columns_to_drop = ['timestamp']
    df_spark = df.drop(*columns_to_drop)
    return df_spark


def delColumnsWithTheSameValue(df):  # УДАЛЕНИЕ СТОЛБЦОВ С ОДИНАКОВЫМ ЗНАЧЕНИЕМ
    count_distinct_df = df.select([approx_count_distinct(x).alias("{0}".format(x)) for x in df.columns])
    dict_of_columns = count_distinct_df.toPandas().to_dict(orient='list')
    # сохранение колонок, в которых только 1 значение
    distinct_columns = [k for k, v in dict_of_columns.items() if v == [1]]
    df_spark = df.drop(*distinct_columns)
    return df_spark


def NanSwupNull(df):  # Замена Nan на Null
    df_spark = df.replace(float('nan'), None)
    return df_spark


def fill_with_mean(this_df, exclude=set()):  # ЗАМЕНА NULL НА СРЕДНЕЕ ЗНАЧЕНИЕ
    stats = this_df.agg(*(avg(c).alias(c) for c in this_df.columns if c not in exclude))
    return this_df.na.fill(stats.first().asDict())


# df_spark = delTime(df_spark)
# df_spark = NanSwupNull(df_spark)
# df_spark = fill_with_mean(df_spark, [])
# df_spark = delColumnsWithTheSameValue(df_spark)


def getJsonOfQuery(prometheus_metric: str):
    # по названию метрики из прометеуса возвращает json данных (в виде списка)
    response = requests.get(f'http://localhost:9090/api/v1/query?query={prometheus_metric}[24h]')
    json_data = response.json()
    return json_data


def getJsonOfFile(file_name: str):
    # возвращает json данных (в виде списка) из файла историчных данных
    with open(f'{file_name}.json', 'r') as j:
        json_data = json.load(j)
        # print(json_data)
    return json_data


def getResourceDict(json_data: json):
    # по листу метрики из json данных возвращает словарь процессов
    # Пока не используется
    list_data = json_data['data']['result']  # список словарей по процессам
    # print(list_data[:3])
    resourceTypeDict = dict(map(lambda x: (x["metric"]["resource_type"], x["values"]), list_data))
    # iterObj = iter(resourceTypeDict)
    # print(list(iterObj))
    return resourceTypeDict


def dictOfJsons(json_data, column_count):
    # возвращает словарь типа: key = timestamp, value = {timestamp:t1, Metric1: V1 ...}
    list_data = json_data['data']['result']
    dict_timestamp = dict()
    for metric in list_data[:column_count]:
        for t in metric['values']:
            dict_timestamp[t[0]] = {}
    for metric in list_data[:column_count]:
        for t in metric['values']:
            dict_timestamp[t[0]]['timestamp'] = t[0]
            dict_timestamp[t[0]][metric['metric']['resource_type']] = float(t[1])
    # print(f"Size dict_timestamp: {len(list(dict_timestamp))}"
    #       f"\n Size uniq value {len(set(dict_timestamp))}")
    return dict_timestamp


def createDF(dict_timestamp: dict, spark):
    # возвращает объединенный df из RDD
    list_of_dicts = dict_timestamp.values()
    rdd = spark.sparkContext.parallelize(list_of_dicts)
    df = rdd.toDF()
    return df


def getJoinedPDF(resourceTypeDict: dict, column_count: int):
    # возвращает склеенный pandas df
    # Пока не используется
    column_list = list(resourceTypeDict.keys())
    print(len(column_list))
    pdf = None
    for el in column_list[:column_count]:  # изменять срез для получения большего кол-ва столбцов
        if pdf is None:
            pdf = pd.DataFrame(resourceTypeDict[el], columns=["timestamp", el])
        else:
            pdf_right = pd.DataFrame(resourceTypeDict[el], columns=["timestamp", el])
            pdf = pdf.join(pdf_right.set_index("timestamp"), on="timestamp")
    pdf = pdf.astype(float)
    print(f"#### Columns: {len(pdf.columns)} #### ")
    return pdf


def buildKMeans(spark_df, k: int):
    # возвращает центроиды кластеров
    vecAssembler = VectorAssembler(inputCols=spark_df.columns[:-1], outputCol="features")
    df_kmeans = vecAssembler.transform(spark_df).select('timestamp', 'features')
    df_kmeans.show()
    kmeans = KMeans().setK(k).setSeed(1).setFeaturesCol("features")
    model = kmeans.fit(df_kmeans)
    centers = model.clusterCenters()
    print("Cluster Centers: ")
    for center in centers:
        print(center)

    return centers


def MethodSilhouette(spark_df, k_max: int):
    # возвращает оптимальное кол-во кластеров
    k_max += 1
    vecAssembler = VectorAssembler(inputCols=spark_df.columns[:-1], outputCol="features")
    df_kmeans = vecAssembler.transform(spark_df).select('timestamp', 'features')
    cost = np.zeros(k_max)
    for k in range(2, k_max):
        kmeans = KMeans().setK(k).setSeed(1).setFeaturesCol("features")
        model = kmeans.fit(df_kmeans)
        evaluator = ClusteringEvaluator()
        pred = model.transform(df_kmeans)
        cost[k] = evaluator.evaluate(pred)
    max_silhouette = max(cost)
    k_opt = list(cost).index(max_silhouette)
    fig, ax = plt.subplots(1, 1, figsize=(8, 6))
    ax.plot(range(2, k_max), cost[2:k_max])
    ax.set_xlabel('k')
    ax.set_ylabel('cost')
    plt.plot(k_opt, max_silhouette, 'o-r', alpha=0.7, label="first", lw=5, mec='b', mew=2, ms=10)
    plt.show()
    return k_opt


spark = SparkSession.builder \
    .master("local") \
    .appName("HistData") \
    .config("spark.debug.maxToStringFields", "1000") \
    .config("spark.driver.memory", "4G")\
    .getOrCreate()
#   spark.conf.set("spark.sql.debug.maxToStringFields", "1000")

file_name = 'query'
json_data = getJsonOfFile(file_name)
print("1")
spark_df = createDF(dictOfJsons(json_data, 100), spark)  # вместо 100 указать кол-во необходимых столбцов
print("2")
#spark_df.show()
df_rename = delPoints(spark_df)
df_without_nan = NanSwupNull(df_rename)
df_avg_null = fill_with_mean(df_without_nan, [])
print(f"Размер датафрейма с усредненными пропусками и переименнованными "
      f"столбцами: {df_avg_null.count()}x{len(df_avg_null.columns)}")
df_cut = delColumnsWithTheSameValue(df_avg_null)
print(f"Размер датафрейма с обрезанными константными столбцами: "
      f"{df_cut.count()}x{len(df_cut.columns)}")

k = MethodSilhouette(df_cut, 10)  # 8- максимальное кол-во кластеров для метода силуэтов
print("Оптимальное значение k: ", k)
