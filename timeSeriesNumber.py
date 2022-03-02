#%%
#%%
from initailFunctionsPath import *

PATH_TRADE = "/home/user1/Data/"
PATH_PORTFOLIO = "/home/user1/Data/Portfolio/{}"
PRICE_PATH = "/home/user1/Data/"
VALID_SYMBOLS_PATH = "/home/user1/Data/"
PATH_OUTPUT = "/home/user1/Data/Outputs/"
#%%
conf = SparkConf()
(
    conf
    # .set('spark.driver.memory', '130g')
    # .set('spark.executer.cores', '58')
    # .set('spark.shuffle.service.index.cache.size', '1g')
    .setAppName("Practice")
)
sc = SparkContext.getOrCreate(conf=conf)
spark = SparkSession(sc)
#%%
price_df = (
    spark.read.parquet(PRICE_PATH.format("Cleaned_Stock_Prices_14001127.parquet"))
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

price_df = replace_arabic_characters_and_correct_symbol_names(price_df)

display_df(price_df)
#%%
dates_list = (
    price_df.select("date")
    .distinct()
    .orderBy("date")
    .rdd.flatMap(lambda x: x)
    .collect()
)

print(dates_list)
#%%
valid_symbols_df = spark.read.parquet(VALID_SYMBOLS_PATH.format("validSymbols.parquet"))

display_df(valid_symbols_df)
#%%
flat_trade_df = spark.read.parquet(PATH_TRADE.format("flat_trade_df.parquet"))
display_df(flat_trade_df)
#%%
daily_portfolio_df = spark.read.parquet(
    PATH_PORTFOLIO.format("daily_portfolio_df.parquet")
)
display_df(daily_portfolio_df)
#%%
portfolio_df = spark.read.parquet(PATH_PORTFOLIO.format("portfolio_df.parquet"))
display_df(portfolio_df)

portfolio_df = portfolio_df.join(valid_symbols_df, on=["symbol"], how="inner")

display_df(portfolio_df)
#%%
nStocksWithinPortfolioOfAllInvestors = []
nInvestors = []
from tqdm import tqdm

for date in tqdm(dates_list):
    result = (
        daily_portfolio_df.filter(F.col("date") <= date)
        .withColumn(
            "rowNumber",
            F.row_number().over(
                Window.partitionBy("accountId", "symbol").orderBy("date")
            ),
        )
        .withColumn(
            "maxRowNumber",
            F.max("rowNumber").over(Window.partitionBy("accountId", "symbol")),
        )
        .filter(F.col("rowNumber") == F.col("maxRowNumber"))
        .filter((F.col("heldShares") > 0) & (F.col("heldShares").isNotNull()))
        .groupBy("accountId")
        .agg(
            F.count(F.lit(1)).alias("nStocksWithinPortfolioOfAllInvestors"),
        )
        .agg(
            F.round(F.mean("nStocksWithinPortfolioOfAllInvestors"), 3).alias(
                "nStocksWithinPortfolioOfAllInvestors"
            ),
            F.count(F.lit(1)).alias("nInvestors"),
        )
    )
    nStocksWithinPortfolioOfAllInvestors.append(result.collect()[0][0])
    nInvestors.append(result.collect()[0][1])
#%%
n_stocks_df = spark.createDataFrame(
    pd.DataFrame(
        {
            "date": dates_list,
            "nStocksWithinPortfolioOfAllInvestors": nStocksWithinPortfolioOfAllInvestors,
            "nInvestors": nInvestors,
        }
    )
)

display_df(n_stocks_df)
#%%
n_stocks_df.write.mode("overwrite").parquet(
    PATH_OUTPUT + "mean_number_of_stocks_within_portfolio.parquet"
)
#%%
initial_ids = [
    row["accountId"] for row in portfolio_df.select("accountId").distinct().collect()
]
initial_ids = set(initial_ids)

#%%
unique_id_trade = flat_trade_df.dropDuplicates(subset=["accountId", "date"])
unique_id_trade.count()
result = {}
for date in dates_list[:3]:
    print(len(initial_ids))
    tempt = (
        unique_id_trade.filter(F.col("date") == date)
        .select("accountId")
        .distinct()
        .collect()
    )
    teades_ids = set([row["accountId"] for row in tempt])
    result[date] = len(teades_ids - initial_ids)

    initial_ids = set.union(initial_ids, teades_ids)
