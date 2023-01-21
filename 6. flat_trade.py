#%%
from initailFunctionsPath import *
#%%

# Loading valid symbols
valid_symbols_df = spark.read.parquet(
    VALID_SYMBOLS_PATH + "{}".format("validSymbols.parquet")
)

display_df(valid_symbols_df)
#%%

# Loading raw trade dataframe
raw_trade_df = spark.read.parquet(PATH_TRADE + "mergedCleanedTradeData.parquet")
display_df(raw_trade_df)
#%%

# Keeping just transaction data of valid symbol
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

# Each row of raw_trade_df has the buyer and seller at the same time. We break it into two dtaframes in order to make it 
# easier to use; buy and sell. Then we union these 2 dataframes for further usage
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

# Each row of this dataframe contains the transaction data for only 1 account
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
