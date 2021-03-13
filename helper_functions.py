import pandas as pd
import numpy as np
import findspark
import re
from functools import reduce
from pyspark import SparkContext
from pyspark.sql import SparkSession, Window, Row
from pyspark.sql.functions import *
from pyspark.sql.types import *
import seaborn as sns
from matplotlib import pyplot as plt
pd.set_option("display.max_columns", None)

# I will be using pyspark for data loading and processing

# create entry
spark = SparkSession.builder.appName("MarchMadness").getOrCreate()

def load_dataframe(file_path):
    """read file from relative path."""
    df = spark.read.option('header', True).format('csv').load(file_path)
    return df

def find_columns(pattern, dataframe):
    df_cols = dataframe.columns
    cols = [column for column in df_cols if re.search(pattern, column)]
    return cols

def check_nulls(df):
    df = df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns]).toPandas().T
    if df[df!=0].dropna().empty:
        print('Dataframe has no null values')
    else:
        return df[df[df!=0].dropna()]

def partion_data(dataframe,order):
    
    window = Window.partitionBy('Season').orderBy(col(order).desc())
    df = dataframe.select('Season', 'TeamID', 'Name', order) \
                       .withColumn('Rank_' + order, row_number().over(window))
    return df 