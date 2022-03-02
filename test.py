#%%
from initailFunctionsPath import *

PATH_TRADE = "/home/user1/Data/{}"
PATH_PORTFOLIO = "/home/user1/Data/Portfolio/{}"
PRICE_PATH = "/home/user1/Data/{}"
VALID_SYMBOLS_PATH = "/home/user1/Data/{}"
PATH_OUTPUT = "/home/user1/Data/Outputs/{}"

#%%
return_df = pd.read_parquet(PATH_OUTPUT.format("return_output.parquet"))
daily_portfolio_df = pd.read_parquet(
    PATH_PORTFOLIO.format("daily_portfolio_df.parquet")
)
portfolio_df = pd.read_parquet(PATH_PORTFOLIO.format("portfolio_df.parquet"))
price_df = pd.read_parquet(PRICE_PATH.format("Cleaned_Stock_Prices_14001127.parquet"))[
    ["name", "jalaliDate", "close_price", "close_price_Adjusted"]
]
adjusted_raw_flat_trade_df = pd.read_parquet(
    PRICE_PATH.format("adjusted_raw_flat_trade_df.parquet")
)
raw_flat_trade_df = pd.read_parquet(PRICE_PATH.format("raw_flat_trade_df.parquet"))
return_df.tail()
#%%
valid_symbols_df = pd.read_parquet(VALID_SYMBOLS_PATH.format("validSymbols.parquet"))
flat_trade_df = pd.read_parquet(PATH_TRADE.format("flat_trade_df.parquet"))
trade_df = pd.read_parquet(PATH_TRADE.format("trade_df.parquet"))
#%%
return_df[return_df["return"] > return_df["return"].quantile(0.0)].head()
#%%
account = "0800A4B5-12A6-47FB-9520-E68FA7248F3C"
symbol = "همراه"
daily_portfolio_df[daily_portfolio_df.accountId == account]
#%%
portfolio_df[portfolio_df.accountId == account]
#%%
trade_df[trade_df.sellerAccountId == account]
#%%
price_df[
    (price_df.name == "دسیناح")
    & (
        (price_df.jalaliDate == 13980105)
        | (price_df.jalaliDate == 13980329)
        | (price_df.jalaliDate == 13980216)
    )
    # & ((price_df.jalaliDate > 13980105) & (price_df.jalaliDate < 13980329))
]
#%%
high_return_firms = [
    "شمواد",
    "کرمان",
]
