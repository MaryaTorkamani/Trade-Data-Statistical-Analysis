# We do the imports, define commenly used variables, paths, and functions here. The we will import this file in each notebook
#%%

from pyspark import SparkConf, SparkContext
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
# import pyarrow.parquet as pq
import os
from pyspark.sql import SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F
from functools import reduce
from pyspark.sql.window import Window
#%%

PATH_TRADE = r"C:/Users/Administrator/Heidari_Ra/Data/raw_trade/"
PRICE_PATH = r"C:/Users/Administrator/Heidari_Ra/Data/"
VALID_SYMBOLS_PATH = r"C:/Users/Administrator/Heidari_Ra/Data/"
PATH_OUTPUT = r"C:/Users/Administrator/Heidari_Ra/Outputs/"
PATH_PORTFOLIO = PRICE_PATH
#%%

# Setting spark configuration
conf = SparkConf()
conf.set("spark.driver.memory", "130g").set(
    "spark.shuffle.service.index.cache.size", "1g"
).setAppName(
    "Practice"
).set('spark.executer.cores', '58')
sc = SparkContext.getOrCreate(conf=conf)
spark = SparkSession(sc)
#%%

HOUR_SECONDS = 60 * 60
MINUTE_SECONDS = 60

MIN_ANALYSIS_DATE = 13980101
MAX_ANALYSIS_DATE = 14001128

N_QUANTILES = 10
#%%

# This function is used to display 3 first rows of each dataframe and persist each dataframe in order to 
# cache the intermediate results in specified storage levels so that any operations on persisted results would improve the performance 
#in terms of memory usage and time
def display_df(df):
    df.persist()
    print(df.count())
    df.show(3, False)

# Returns the first and last date of each dataframe
def min_max(df):
    return df.agg(
        F.min("date").alias("min_date"), F.max("date").alias("max_date")
    ).show()

# Generating a modified time
def modify_time(x):
    hour = x // 10000
    minute = (x % 10000) // 100
    second = x % 100
    return HOUR_SECONDS * 3600 + MINUTE_SECONDS * 60 + second

modify_time_udf = F.udf(modify_time, T.IntegerType())

dropSpace = F.udf(lambda x: x.replace(" ", ""), T.StringType()) # Removing the space

# Changing the symbols and Arabic characters
mappingDict = {
    "ما  ": "ما",
    "جم  ": "جم",
    "جمپیلن": "جم پیلن",
    "افقملت": "افق ملت",
    "آسپ": "آ س پ",
    "آپ  ": "آپ",
    "سپ  ": "سپ",
    "غپاذر": "غپآذر",
    "هدشت": "دهدشت",
    "نگان": "زنگان",
    "فبورس": "فرابورس",
    "شیری": "دشیری",
    "وتعان": "وتعاون",
    "آس پ": "آ س پ",
    "انرژی1": "انرژی 1",
    "انرژی2": "انرژی 2",
    "انرژی3": "انرژی 3",
    "انرژیح1": "انرژیح 1",
    "انرژیح2": "انرژیح 2",
    "انرژیح3": "انرژیح 3",
    "فناوا": "فن آوا",
    "فنآوا": "فن آوا",
    "امینیکم": "امین یکم",
    "هایوب": "های وب",
    "کیبیسی": "کی بی سی",
    "کیبیسیح": "کی بی سیح",
    "واتوس": "وآتوس",
}

# Defining a function to change the Arabic characters with Persian ones
def replace_arabic_characters_and_correct_symbol_names(data):
    mapping = {
        "ك": "ک",
        "گ": "گ",
        "دِ": "د",
        "بِ": "ب",
        "زِ": "ز",
        "ذِ": "ذ",
        "شِ": "ش",
        "سِ": "س",
        "ى": "ی",
        "ي": "ی",
    }
    for i in mapping:
        data = data.withColumn("symbol", F.regexp_replace("symbol", i, mapping[i]))
    data = (
        data.withColumn(
            "symbol",
            F.when(
                (F.col("symbol").substr(1, 1) == "ذ") & (F.col("symbol") != "ذوب"), # removing extra characters
                F.col("symbol").substr(2, 30),
            ).otherwise(F.col("symbol")),
        )
        .withColumn(
            "symbol",
            F.when(
                F.col("symbol").substr(1, 2) == "گژ", F.col("symbol").substr(3, 30) # removing extra characters
            ).otherwise(F.col("symbol")),
        )
        .withColumn(
            "symbol",
            F.when(
                F.col("symbol").substr(1, 1) == "ژ", F.col("symbol").substr(2, 30) # removing extra characters
            ).otherwise(F.col("symbol")),
        )
        .replace(mappingDict, subset=["symbol"])
    )
    return data

# Removing the space between characters
spaceDeleteUDF1 = F.udf(lambda s: s.replace("\u200d", ""), T.StringType()) 
spaceDeleteUDF2 = F.udf(lambda s: s.replace("\u200c", ""), T.StringType())
#%%

# This function creates portfolio by adding up shares, cash deposited to and cash withdrawn from the market
def make_daily_portfolio():
    window = (
        Window.partitionBy("accountId", "symbol")
        .orderBy("date")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    return (
        F.sum("nHeldShares").over(window),
        F.sum("cashOut").over(window),
        F.sum("cashIn").over(window),
    )

# This function creates portfolio by adding up shares, cash deposited to and cash withdrawn from the market per broker
def make_daily_portfolio_broker():
    window = (
        Window.partitionBy("broker", "symbol")
        .orderBy("date")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    return (
        F.sum("nHeldShares").over(window),
        F.sum("cashOut").over(window),
        F.sum("cashIn").over(window),
    )
