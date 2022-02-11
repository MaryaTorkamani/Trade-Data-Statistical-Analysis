#%%
from initailFunctionsPath import *

#%%
conf = SparkConf()
conf.set("spark.driver.memory", "200g").set(
    "spark.shuffle.service.index.cache.size", "1g"
).set('spark.executer.cores', '80').setAppName(
    "Practice"
)  # 
sc = SparkContext.getOrCreate(conf=conf)
spark = SparkSession(sc)

#%%
valid_symbols_df = spark.read.parquet(
    VALID_SYMBOLS_PATH + "/{}".format("validSymbols.parquet")
)


display_df(valid_symbols_df)

#%%

raw_trade_df = spark.read.parquet(VALID_SYMBOLS_PATH + "/mergedCleanedTradeData.parquet")

display_df(raw_trade_df)

#%%
trade_df = (
    raw_trade_df
    .join(valid_symbols_df, on=["symbol"], how="inner")
    .select(
        "date",
        # "time",
        # "secondsWithinDay",
        "symbol",
        "nTradeShares",
        "tradePrice",
        "tradeSettlementValue",
        "buyerAccountId",
        "sellerAccountId",
        "buyerBroker",
        "sellerBroker",
        "buyerBrokerGroup",
        "sellerBrokerGroup",
    )
)

display_df(trade_df)
del raw_trade_df

#%%
buy_trade_df = trade_df.select(
    "date",
    "symbol",
    F.col("buyerAccountId").alias("accountId"),
    "nTradeShares",
    (-F.col("tradeSettlementValue")).alias("settlementValue"),
)

sell_trade_df = trade_df.select(
    "date",
    "symbol",
    F.col("sellerAccountId").alias("accountId"),
    (-F.col("nTradeShares")).alias("nTradeShares"),
    F.col("tradeSettlementValue").alias("settlementValue"),
)

raw_flat_trade_df = (
    buy_trade_df
    .union(sell_trade_df)
    # .groupBy(["date", "symbol", "accountId"])
    # .agg(
    #     F.sum("nTradeShares").alias("nTradeShares"),
    #     F.sum(F.when(F.col("settlementValue") > 0, F.col("settlementValue"))).alias(
    #         "cashOut"
    #     ),
    #     F.sum(F.when(F.col("settlementValue") < 0, F.col("settlementValue"))).alias(
    #         "cashIn"
    #     ),
    # )
    # .fillna(0, subset=["cashOut", "cashIn"])
    .select(
        "date",
        "symbol",
        "accountId",
        "nTradeShares",
        "settlementValue",
        (F.when(F.col("settlementValue") > 0, F.col("settlementValue"))).alias(
            "cashOut"
        ),
        (F.when(F.col("settlementValue") < 0, F.col("settlementValue"))).alias(
            "cashIn"
        ),
    )
    .fillna(0, subset=["cashOut", "cashIn"])
    .orderBy("date", "accountId")
)

display_df(raw_flat_trade_df)
del buy_trade_df
del sell_trade_df
#%%
trade_df.write.mode("overwrite").parquet(VALID_SYMBOLS_PATH + "/{}".format("trade_df.parquet"))

#%%
raw_flat_trade_df.write.mode("overwrite").parquet(
    VALID_SYMBOLS_PATH + "/{}".format("raw_flat_trade_df.parquet")
)
# %%
