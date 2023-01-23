#%%
from initailFunctionsPath import *
#%%


print(MIN_ANALYSIS_DATE, MAX_ANALYSIS_DATE)
price_df = (
    spark.read.parquet(PRICE_PATH + "{}".format("Cleaned_Stock_Prices_14001127.parquet"))
    .filter(F.col("jalaliDate").between(MIN_ANALYSIS_DATE, MAX_ANALYSIS_DATE))
    .select(
        F.col("jalaliDate").alias("date"),
        F.col("name").alias("symbol"),
        "close_price",
        "close_price_adjusted",
        "shrout",
        (F.col("MarketCap") / 10**7).alias("mktcap"),
    )
    .dropDuplicates()
)

display_df(price_df)
#%%

min_max(price_df)
#%%

MIN_PRICE_DATE = price_df.agg(F.min("date")).collect()[0][0]
MAX_PRICE_DATE = price_df.agg(F.max("date")).collect()[0][0]

price_df.agg(F.countDistinct("symbol")).show()
price_df.filter(F.col("date") == MIN_PRICE_DATE).agg(F.countDistinct("symbol")).show()
price_df.filter(F.col("date") == MAX_PRICE_DATE).agg(F.countDistinct("symbol")).show()
#%%

valid_symbols_df = spark.read.parquet(
    VALID_SYMBOLS_PATH + "/{}".format("validSymbols.parquet")
)

display_df(valid_symbols_df)
#%%

trade_df = spark.read.parquet(VALID_SYMBOLS_PATH + "/{}".format("trade_df.parquet"))
display_df(trade_df)
# Note: 'time' columns is not reliable!
#%%

print(
    "missing nTradeShares: ",
    round(trade_df.filter(F.col("nTradeShares") == 0).count() / trade_df.count(), 5),
)
print(
    "missing tradeSettlementValue: ",
    round(
        trade_df.filter(F.col("tradeSettlementValue") == 0).count() / trade_df.count(),
        5,
    ),
)
#%%

portfolio_df = spark.read.parquet(PATH_PORTFOLIO + "{}".format("portfolio_df.parquet"))
display_df(portfolio_df)
#%%

portfolio_df = portfolio_df.join(valid_symbols_df, on=["symbol"], how="inner")
display_df(portfolio_df)
#%%

portfolio_df.filter(F.col("nHeldShares") < 0).count()
#%%

price_symbols = price_df.select("symbol").distinct().withColumn("price", F.lit(1))
trade_symbols = trade_df.select("symbol").distinct().withColumn("trade", F.lit(1))
portfolio_symbols = (
    portfolio_df.select("symbol").distinct().withColumn("portfolio", F.lit(1))
)

symbols_df = trade_symbols.join(portfolio_symbols, on=["symbol"], how="outer").join(
    price_symbols, on=["symbol"], how="outer"
)

print(
    symbols_df.filter(F.col("price").isNull())
    .select("symbol")
    .rdd.flatMap(lambda x: x)
    .collect()
)
#%%

common_investors_df = (
    trade_df.select(F.col("buyerAccountId").alias("accountId"))
    .union(trade_df.select(F.col("sellerAccountId").alias("accountId")))
    .dropDuplicates()
    .withColumn("trade", F.lit(1))
    .join(
        portfolio_df.select("accountId", F.lit(1).alias("portfolio")).dropDuplicates(),
        on=["accountId"],
        how="outer",
    )
    .fillna(0, subset=["trade", "portfolio"])
)

display_df(common_investors_df)
#%%

trade_only = common_investors_df.filter(
    (F.col("trade") == 1) & (F.col("portfolio") == 0)
).count()
all_trade = common_investors_df.filter(F.col("trade") == 1).count()

print(
    "share of missing portfolio accounts among traders:",
    round(100 * trade_only / all_trade, 2),
    "%",
)
# It seems reasonable to attribute this missing portion to the new entrants!
#%%

portfolio_only = common_investors_df.filter(
    (F.col("trade") == 0) & (F.col("portfolio") == 1)
).count()
all_portfolio = common_investors_df.filter(F.col("portfolio") == 1).count()

print(
    "share of missing trades among investors who have nitial portfolio:",
    round(100 * portfolio_only / all_portfolio, 2),
    "%",
)
# It seems reasonable to attribute this missing portion to the new entrants!
#%%

(
    trade_df.select(F.col("buyerAccountId").alias("accountId"))
    .union(trade_df.select(F.col("sellerAccountId").alias("accountId")))
    .dropDuplicates()
    .count()
)
#%%

mass_public_stocks_df = (
    portfolio_df.groupBy("symbol", "nHeldShares")
    .agg(F.countDistinct("accountId").alias("nHolders"))
    .withColumn("nAllHolders", F.sum("nHolders").over(Window.partitionBy("symbol")))
    .withColumn(
        "rank",
        F.row_number().over(Window.partitionBy("symbol").orderBy(F.desc("nHolders"))),
    )
    .filter(F.col("rank") == 1)
    .drop("rank")
    .orderBy(F.desc("nHolders"))
    .withColumn("shareOfHolders", F.round(F.col("nHolders") / F.col("nAllHolders"), 3))
    #     .join(price_df.filter(F.col('date') == 13980105).select('symbol', 'shrout'), on = 'symbol', how = 'left')
    #     .withColumn('shareOfShares', F.round(F.col('nHeldShares')*F.col('nHolders') / F.col('shrout'), 3))
)

display_df(mass_public_stocks_df)
#%%

raw_flat_trade_df = spark.read.parquet(
    PATH_PORTFOLIO + "{}".format("raw_flat_trade_df.parquet")
)

display_df(raw_flat_trade_df)
#%%

print(raw_flat_trade_df.filter(F.col("nTradeShares") == 0).count())
print(trade_df.filter(F.col("tradeSettlementValue") == 0).count())
#%%

print(raw_flat_trade_df.filter(F.col("cashIn") > 0).count())
print(raw_flat_trade_df.filter(F.col("cashOut") < 0).count())
#%%

print(price_df.count())
adjustment_ratio = price_df.withColumn(
    "ratio", F.col("close_price_adjusted") / F.col("close_price")
).select(
    F.col("date"),
    F.col("symbol"),
    F.col("ratio"),
)
display_df(adjustment_ratio)
#%%

print(portfolio_df.count())
adjusted_portfolio_df = (
    portfolio_df.join(adjustment_ratio, on=["date", "symbol"])
    .fillna(1)
    .withColumn("nHeldShares", F.col("nHeldShares") / F.col("ratio"))
    .drop("ratio")
)
display_df(adjusted_portfolio_df)
#%%

print(raw_flat_trade_df.count())
adjusted_raw_flat_trade_df = (
    raw_flat_trade_df.join(adjustment_ratio, on=["date", "symbol"])
    .withColumn("nTradeShares", F.col("nTradeShares") / F.col("ratio"))
    .drop("ratio")
)
display_df(adjusted_raw_flat_trade_df)
#%%

raw_daily_portfolio_df = (
    adjusted_portfolio_df
    # portfolio_df
    .select(
        "date",
        "symbol",
        "accountId",
        "nHeldShares",
        F.lit(0).alias("cashOut"),
        F.lit(0).alias("cashIn"),
    )
    .union(
        adjusted_raw_flat_trade_df
        .select(
        "date",
        "symbol",
        "accountId",
        "nTradeShares",
        "cashOut",
        "cashIn"
    )
        # raw_flat_trade_df
        .withColumnRenamed("nTradeShares", "nHeldShares")
    )
    .groupBy("date", "symbol", "accountId")
    .agg(
        F.sum("nHeldShares").alias("nHeldShares"),
        F.sum("cashOut").alias("cashOut"),
        F.sum("cashIn").alias("cashIn"),
    )
    .orderBy("accountId", "date")
    .withColumn("heldShares", make_daily_portfolio()[0])
    .withColumn("netCashOut", make_daily_portfolio()[1])
    .withColumn("netCashIn", make_daily_portfolio()[2])
    .drop("nHeldShares", "settlementValue", "cashIn", "cashOut")
)

display_df(raw_daily_portfolio_df)
#%%

invalid_holdings_df = (
    raw_daily_portfolio_df.filter(F.col("heldShares") < 0)
    .select("accountId", "symbol")
    .dropDuplicates()
    .withColumn("invalidHolding", F.lit(1))
)
display_df(invalid_holdings_df)
#%%

raw_daily_portfolio_df.write.mode("overwrite").parquet(
    PATH_PORTFOLIO + "{}".format("daily_portfolio_df.parquet")
)
#%%

adjusted_portfolio_df.write.mode("overwrite").parquet(
    PATH_PORTFOLIO + "{}".format("adjusted_portfolio_df.parquet")
)
#%%

invalid_holdings_df.write.mode("overwrite").parquet(
    PATH_PORTFOLIO + "{}".format("invalid_holdings_df.parquet")
)
display_df(invalid_holdings_df)
# %%

adjusted_raw_flat_trade_df.write.mode("overwrite").parquet(
    PATH_PORTFOLIO + "{}".format("adjusted_raw_flat_trade_df.parquet")
)
#%%

mass_public_stocks_df.write.mode("overwrite").parquet(
    PATH_PORTFOLIO + 'mass_public_stocks.parquet'
)
#%%
