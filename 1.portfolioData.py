from initailFunctionsPath import *
#%%

raw_portfolio_df = spark.read.parquet(PATH_PORTFOLIO + "{}".format("raw_portfolio.parquet"))
display_df(raw_portfolio_df)
#%%
# for i in raw_portfolio_df.columns:
#     raw_portfolio_df = raw_portfolio_df.withColumn(i, spaceDeleteUDF1(i)).withColumn(
#         i, spaceDeleteUDF2(i)
#     )


#%%
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
raw_portfolio_df.write.mode("overwrite").parquet(
    PATH_PORTFOLIO + "{}".format("raw_portfolio_df.parquet")
)
#%%
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

# portfolio_df = replace_arabic_characters_and_correct_symbol_names(portfolio_df)
display_df(portfolio_df)


replaceChar = F.udf(lambda s: s[:-1], T.StringType())


def agg(x):
    t = ""
    for i in x.split(" "):
        t += i
    return t


def cleaning(data):
    data = data.withColumn(
        "symbol",
        F.when(F.col("symbol").endswith("ج"), replaceChar(F.col("symbol"))).otherwise(
            F.col("symbol")
        ),
    )
    for i in ["اوج", "بکهنوج", "ساروج", "نبروج", "وسخراج"]:
        data = data.withColumn(
            "symbol", F.when(F.col("symbol") == i[:-1], i).otherwise(F.col("symbol"))
        )
    data = data.dropDuplicates()
    return data


portfolio_df = cleaning(portfolio_df)

#%%
portfolio_df = portfolio_df.groupBy(["accountId", "date", "symbol"]).agg(
    F.sum("nHeldShares").alias("nHeldShares")
)

display_df(portfolio_df)
#%%
portfolio_df.write.mode("overwrite").parquet(
    PATH_PORTFOLIO + "{}".format("portfolio_df.parquet")
)
#%%
