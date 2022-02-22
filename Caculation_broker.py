#%%
from initailFunctionsPath import *

#%%
conf = SparkConf()
conf.set("spark.driver.memory", "130g").set(
    "spark.shuffle.service.index.cache.size", "1g"
).setAppName(
    "Practice"
)  # .set('spark.executer.cores', '58')
sc = SparkContext.getOrCreate(conf=conf)
spark = SparkSession(sc)
#%%
valid_symbols_df = spark.read.parquet(
    VALID_SYMBOLS_PATH + "{}".format("validSymbols.parquet")
)


display_df(valid_symbols_df)

#%%
raw_trade_df = spark.read.parquet(PATH_TRADE + "mergedCleanedTradeData.parquet")

display_df(raw_trade_df)

trade_df = (
    raw_trade_df.withColumn("secondsWithinDay", modify_time_udf("time"))
    .join(valid_symbols_df, on=["symbol"], how="inner")
    .select(
        "date",
        "time",
        "secondsWithinDay",
        "symbol",
        "nTradeShares",
        "tradePrice",
        "tradeSettlementValue",
        dropSpace(F.col("buyerAccountId")).alias("buyerAccountId"),
        dropSpace(F.col("sellerAccountId")).alias("sellerAccountId"),
        "buyerBroker",
        "sellerBroker",
        "buyerBrokerGroup",
        "sellerBrokerGroup",
    )
)

display_df(trade_df)


#%%
buy_trade_df = trade_df.select(
    "date",
    "symbol",
    "nTradeShares",
    (-F.col("tradeSettlementValue")).alias("settlementValue"),
    F.col("buyerBroker").alias("broker"),
    F.col("buyerBrokerGroup").alias("brokerGroup"),
)

sell_trade_df = trade_df.select(
    "date",
    "symbol",
    (-F.col("nTradeShares")).alias("nTradeShares"),
    F.col("tradeSettlementValue").alias("settlementValue"),
    F.col("sellerBroker").alias("broker"),
    F.col("sellerBrokerGroup").alias("brokerGroup"),
)

raw_flat_trade_df = (
    buy_trade_df.union(sell_trade_df)
    .groupBy(["date", "symbol", "broker"])
    .agg(
        F.sum("nTradeShares").alias("nTradeShares"),
        F.sum(F.when(F.col("settlementValue") > 0, F.col("settlementValue"))).alias(
            "cashOut"
        ),
        F.sum(F.when(F.col("settlementValue") < 0, F.col("settlementValue"))).alias(
            "cashIn"
        ),
    )
    .fillna(0, subset=["cashOut", "cashIn"])
    .orderBy("date", "broker")
)

display_df(raw_flat_trade_df)

# %%
raw_flat_trade_df.columns


#%%
raw_daily_portfolio_df = (raw_flat_trade_df
    .withColumnRenamed('nTradeShares', 'nHeldShares')
    .groupBy('date', 'symbol', 'broker')
    .agg(
        F.sum('nHeldShares').alias('nHeldShares'),
        F.sum('cashOut').alias('cashOut'),
        F.sum('cashIn').alias('cashIn')
    )
    .orderBy('broker', 'date')
    .withColumn('heldShares', make_daily_portfolio_broker()[0])
    .withColumn('netCashOut', make_daily_portfolio_broker()[1])
    .withColumn('netCashIn', make_daily_portfolio_broker()[2])
    .drop('nHeldShares', 'settlementValue', 'cashIn', 'cashOut')
)

display_df(raw_daily_portfolio_df)

# %%
gain_from_trade_df = (
    raw_flat_trade_df
    .groupBy('broker')
    .agg(
        F.sum('cashOut').alias('netCashOut'),
        F.sum('cashIn').alias('netCashIn'),
    )
)
display_df(gain_from_trade_df)
# %%
price_df = (
    spark.read.parquet(PRICE_PATH.format('Cleaned_Stock_Prices_14001116.parquet'))
    .filter(F.col('jalaliDate').between(MIN_ANALYSIS_DATE, MAX_ANALYSIS_DATE))
    .select(
        F.col('jalaliDate').alias('date'),
        F.col('name').alias('symbol'),
        'close_price',
        'close_price_adjusted',
        'shrout',
        (F.col('MarketCap') / 10**7).alias('mktcap')
    )
    .dropDuplicates()
)

price_df = replace_arabic_characters_and_correct_symbol_names(price_df)

added_price_df = spark.createDataFrame(pd.DataFrame({
                                                        'date' : [13980105],
                                                        'symbol' : ['ومشان'],
                                                        'close_price' : [561],
                                                        'close_price_adjusted' : [np.nan],
                                                        'shrout' : [20000000],
                                                        'mktcap' : [1122]
                                                    })
                                      )

price_df = price_df.union(added_price_df)
MAX_PRICE_DATE = price_df.agg(F.max('date')).collect()[0][0]

#%%
display_df(raw_daily_portfolio_df)


#%%
final_portfolio_value_df = (
    raw_daily_portfolio_df
    .withColumn('rowNumber', F.row_number().over(Window.partitionBy('broker', 'symbol').orderBy('date')))
    .withColumn('maxRowNumber', F.max('rowNumber').over(Window.partitionBy('broker', 'symbol')))
    .filter(F.col('rowNumber') == F.col('maxRowNumber'))
    .filter(F.col('heldShares') > 0)
    .withColumn('date', F.lit(MAX_PRICE_DATE))
    .join(price_df.select('date', 'symbol', 'close_price'), on = ['date', 'symbol'], how = 'left')
    .dropna(subset = ['close_price'])
    .withColumn('value', F.col('heldShares') * F.col('close_price'))
    .groupBy('broker')
    .agg(
        (F.sum('value') / 10**7).alias('finalPortfolioValue')
    )   
)

display_df(final_portfolio_value_df)

# %%
