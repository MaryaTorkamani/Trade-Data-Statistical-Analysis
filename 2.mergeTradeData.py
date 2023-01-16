#%%
from initailFunctionsPath import *

#%%

# columns are defined
columns =  [
    'date'
    ,'ticket'
    ,'symbol'
    ,'buyerAccount'
    ,'sellerAccount'
    ,'buyerBroker'
    ,'sellerBroker'
    ,'buyerBrokerGroup'
    ,'sellerBrokerGroup'
    ,'shares'
    ,'price'
    ,'settlementValue'
    ]

files = [x for x in os.listdir(PATH_TRADE)]# if int(x[15:23]) >= 14000511]
print(files[0])

raw_trade_df = (
    spark.read.parquet(PATH_TRADE + files[0]) 
)

raw_trade_df = raw_trade_df.select(*columns)

# We want to union all files
for file in files[1:]:
    df = (
        spark.read.parquet(PATH_TRADE + file)
        .select(*columns)
    )
    raw_trade_df = raw_trade_df.union(df)
    print(file,df.count())

display_df(raw_trade_df)
#%%

# Settlement value is the total valu of the trade in Rial.
# Some of settlement values are missed when reading from database. We want to replace them with the correct value. We also change the name of the columns.
raw_trade_df = (
    raw_trade_df.withColumn(
        "settlementValue",
        F.when(
            (F.col("settlementValue") == 0) | (F.col("settlementValue").isNull()),
            F.col("price") * F.col("shares"),
        ).otherwise(F.col("settlementValue")),
    )
    .select(
        F.col("date").cast("string"),
        # F.col("time").cast("integer").alias("time"),
        "symbol",
        F.col("buyerAccount").alias("buyerAccountId"),
        F.col("sellerAccount").alias("sellerAccountId"),
        F.col("shares").cast("integer").alias("nTradeShares"),
        F.col("price").alias("tradePrice"),
        F.round(F.col("settlementValue") / 10**7, 7).alias("tradeSettlementValue"),
        F.col("buyerBroker"),
        F.col("sellerBroker"),
        F.col("buyerBrokerGroup"),
        F.col("sellerBrokerGroup"),
    )
    .dropDuplicates()
)
#%%

# Replacing the Arabic character with Persian characters
raw_trade_df = replace_arabic_characters_and_correct_symbol_names(raw_trade_df)

display_df(raw_trade_df)

#%%
# Saving the dataframe into parquet files
raw_trade_df.write.mode('overwrite').parquet(PRICE_PATH + "/mergedCleanedTradeData.parquet")
# %%

