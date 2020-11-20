from pyspark.sql import *
from pyspark import SparkContext, SparkConf
import json
import numpy as np
import requests
from pyspark.sql import dataframe
from pyspark.sql.types import *
import pandas as pd
from IPython.display import display, HTML
from pyspark.sql.functions import avg 
from pyspark.mllib.stat import Statistics

def delTime(df):   # удаление столбца Time
    columns_to_drop = ['Time'] 
    df_spark = df.drop(*columns_to_drop)
    return df_spark

def delColumnsWithTheSameValue(df):   # УДАЛЕНИЕ СТОЛБЦОВ С ОДИНАКОВЫМ ЗНАЧЕНИЕМ
    from pyspark.sql.functions import approx_count_distinct               
    count_distinct_df = df.select([approx_count_distinct(x).alias("{0}".format(x)) for x in df.columns])
    dict_of_columns = count_distinct_df.toPandas().to_dict(orient='list')
    # сохранение колонок, в которых только 1 значение
    distinct_columns=[k for k,v in dict_of_columns.items() if v == [1]]
    df_spark = df.drop(*distinct_columns)
    return df_spark	
	
def NanSwupNull(df):     # Замена Nan на Null
    df_spark = df.replace(float('nan'), None) 
    return df_spark
	
def fill_with_mean(this_df, exclude=set()):   # ЗАМЕНА NULL НА СРЕДНЕЕ ЗНАЧЕНИЕ
    stats = this_df.agg(*(avg(c).alias(c) for c in this_df.columns if c not in exclude))
    return this_df.na.fill(stats.first().asDict())

df_spark = delTime(df_spark)
df_spark = NanSwupNull(df_spark)
df_spark = fill_with_mean(df_spark, [])
df_spark = delColumnsWithTheSameValue(df_spark)