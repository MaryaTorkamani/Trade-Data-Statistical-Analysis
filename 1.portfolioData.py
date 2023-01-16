from initailFunctionsPath import *
#%%

# Loading the raw portfolio read from the server and saved in parquet format
raw_portfolio_df = spark.read.parquet(PATH_PORTFOLIO + "{}".format("raw_portfolio.parquet"))
display_df(raw_portfolio_df)
#%%

# Each one of these strings in the list is a broker. We want to add up each account's assets in brokers to get the total number of shares of each security 
for i in ["SPTROH", "SPBYNS", "SPBYNR", "SPSLNS", "SPPLGE"]:
    raw_portfolio_df = raw_portfolio_df.withColumn(i, F.col(i).cast("int"))
raw_portfolio_df = (
    raw_portfolio_df.withColumn(
        "SPTROH",
        F.col("SPTROH")
        + F.col("SPBYNS")
        + F.col("SPBYNR")
        + F.col("SPSLNS")
        + F.col("SPPLGE"),
    )
    .select("SPSYMB", "SPDATE", "SPACC#", "SPTROH")
    .dropDuplicates()
)
display_df(raw_portfolio_df)
#%%

# Saving the generated dataframe
raw_portfolio_df.write.mode("overwrite").parquet(
    PATH_PORTFOLIO + "{}".format("raw_portfolio_df.parquet")
)
#%%

# Changing the column names
mapping = dict(
    zip(
        ["SPDATE", "SPSYMB", "SPACC#", "SPTROH"],
        ["date", "symbol", "accountId", "nHeldShares"],
    )
)

portfolio_df = raw_portfolio_df.select(
    [F.col(c).alias(mapping.get(c, c)) for c in raw_portfolio_df.columns]
).select(
    "date", "symbol", "accountId", "nHeldShares"
)
#%%

# Replacing Arabic characters with Persian characters
portfolio_df = replace_arabic_characters_and_correct_symbol_names(portfolio_df)
display_df(portfolio_df)


replaceChar = F.udf(lambda s: s[:-1], T.StringType())

# Reamoving the blank spaces between letters in strings
def agg(x):
    t = ""
    for i in x.split(" "):
        t += i
    return t

# Some security symbols have extra characters at their end. We want to remove them
def cleaning(data):
    data = data.withColumn(
        "symbol",
        F.when(F.col("symbol").endswith("ج"), replaceChar(F.col("symbol"))).otherwise(
            F.col("symbol")
        ),
    )
    for i in ["اوج", "بکهنوج", "ساروج", "نبروج", "وسخراج"]: # Some symbols end with removed character. We do not want to change them
        data = data.withColumn(
            "symbol", F.when(F.col("symbol") == i[:-1], i).otherwise(F.col("symbol"))
        )
    data = data.dropDuplicates()
    return data


portfolio_df = cleaning(portfolio_df)
#%%

# We aggregate the total share number of securities for each account
portfolio_df = portfolio_df.groupBy(["accountId", "date", "symbol"]).agg(
    F.sum("nHeldShares").alias("nHeldShares")
)

display_df(portfolio_df)
#%%

# Saving the dataframe into parquet files
portfolio_df.write.mode("overwrite").parquet(
    PATH_PORTFOLIO + "{}".format("portfolio_df.parquet")
)
#%%
