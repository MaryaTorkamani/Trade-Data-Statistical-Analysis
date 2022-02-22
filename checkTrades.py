#%%
import pandas as pd

path = r"/home/user1/Data/{}"
trade_df = pd.read_parquet(path.format("raw_flat_trade_df.parquet"))
return_df = pd.read_parquet(path.format("Esmaeil/return_output.parquet"))
porfolio_df = pd.read_parquet(path.format("Portfolio/portfolio_df.parquet"))
price_df = pd.read_parquet(path.format("Cleaned_Stock_Prices_14001116.parquet"))
#%%
return_df['profit'] = return_df.netCashOut + return_df.netCashIn + return_df.finalPortfolioValue - return_df.initialPortfolioValue

def summary_account(df, columns):
    result = {}
    for i in columns:
        tempt = {}
        tempt["mean"] = df[i].mean()
        # tempt["min"] = df[i].min()
        tempt["0.1%"] = df[i].quantile(0.001)
        tempt["1%"] = df[i].quantile(0.01)
        tempt["10%"] = df[i].quantile(0.1)
        tempt["20%"] = df[i].quantile(0.2)
        tempt["30%"] = df[i].quantile(0.3)
        tempt["median"] = df[i].quantile(0.5)
        tempt["75%"] = df[i].quantile(0.75)
        tempt["90%"] = df[i].quantile(0.9)
        tempt["95%"] = df[i].quantile(0.95)
        tempt["99%"] = df[i].quantile(0.99)
        tempt["99.9%"] = df[i].quantile(0.999)
        # tempt["max"] = df[i].max()
        result[i] = tempt
    return pd.DataFrame.from_dict(result).round(2).T

summary_account(return_df, ['profit'])


#%%
price_df = price_df[(price_df.jalaliDate>13980101)&(price_df.jalaliDate<13980401)]
price_returns = (
    price_df.groupby("name").last()[["close_price_Adjusted"]]
    - price_df.groupby("name").first()[["close_price_Adjusted"]]
) / price_df.groupby("name").first()[["close_price_Adjusted"]]
mapingdict = dict(zip(price_returns.index,price_returns.close_price_Adjusted))
porfolio_df['price_return'] = porfolio_df.symbol.map(mapingdict)
#%%
price_returns = (
    price_df.groupby("name").last()[["close_price"]]
    - price_df.groupby("name").first()[["close_price"]]
) / price_df.groupby("name").first()[["close_price"]]
mapingdict = dict(zip(price_returns.index,price_returns.close_price))
porfolio_df['price_return_unadjusted'] = porfolio_df.symbol.map(mapingdict)

# %%
return_df.sort_values(by=["return"]).tail(10)[["accountId", "return"]]
#%%
account_id = "40AC9C79-C2E0-41AA-8C74-40BFF107E9B4"
# trade_df[trade_df.accountId == account_id]
return_df[return_df["accountId"] == account_id]
porfolio_df[porfolio_df["accountId"] == account_id]
