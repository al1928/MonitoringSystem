# import json
# from json import dumps
# from kafka import KafkaProducer
# from collections import OrderedDict
# import pyspark.sql.functions
# from pyspark.ml.clustering import KMeans
# from pyspark.ml.evaluation import ClusteringEvaluator
# from pyspark.ml.feature import VectorAssembler
# from pyspark.sql.functions import approx_count_distinct
# from pyspark.sql.functions import avg
from pyspark.sql.session import SparkSession


def getShema():
    spark = SparkSession.builder \
        .master("local") \
        .appName("HistData") \
        .config("spark.debug.maxToStringFields", "1000") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    df2 = spark.read.json("one_line.json")
    df2.printSchema()
    # print(df2.schema)
    df = spark.createDataFrame(spark.sparkContext.emptyRDD(), df2.schema)
    spark.stop()
    # print("\n\n\n\n\n\n\n\n\n\n")
    # print(df)

    return df2.schema

# #
# with open("newjson.json", "r") as f:
#     data5 = json.loads(f)
# print(data5)

# f = open('one_line.json')
# data5 = f.read()


#print(data5)

# df2 = spark.read.json("part-00000-794fb5c9-b74f-4cab-b6c9-da54f6a2e45b-c000.json")
# print(df2)

########df = spark.range(len(data5))

# schema = df.select(pyspark.sql.functions.schema_of_json(pyspark.sql.functions.lit(str(data5))).alias("json")).collect() # СОЗДАЁТСЯ СХЕМА DDL
# print(schema)


#df555 = spark.createDataFrame(data=text,schema=schema)



# df555.show(truncate=False)

# = df2._jdf.schema().treeString()

#print(v)




#df555 = spark.createDataFrame(data=data5,schema=df2.schema)
# schema = pyspark.sql.functions.schema_of_json('{"a": 1}')
# print(df.select(schema.alias("json")).collect())

