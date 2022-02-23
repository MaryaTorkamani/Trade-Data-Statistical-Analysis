#%%
from initailFunctionsPath import *

# %%
ind_ins_trade_df = pd.read_parquet(PRICE_PATH + "{}".format("ind_ins_trade_14001127.parquet"))
ind_ins_trade_df = ind_ins_trade_df[
    (ind_ins_trade_df.jalaliDate > MIN_ANALYSIS_DATE) & (ind_ins_trade_df.jalaliDate < MAX_ANALYSIS_DATE)
]
# %%
just_ind_sell_df = ind_ins_trade_df[ind_ins_trade_df.ins_sell_count == 0][
    ["name", "jalaliDate"]
].rename(columns={"jalaliDate": "date", "name": "symbol"})
just_ins_sell_df = ind_ins_trade_df[ind_ins_trade_df.ind_sell_count == 0][
    ["name", "jalaliDate"]
].rename(columns={"jalaliDate": "date", "name": "symbol"})
just_ind_buy_df = ind_ins_trade_df[ind_ins_trade_df.ins_buy_count == 0][
    ["name", "jalaliDate"]
].rename(columns={"jalaliDate": "date", "name": "symbol"})
just_ins_buy_df = ind_ins_trade_df[ind_ins_trade_df.ind_buy_count == 0][
    ["name", "jalaliDate"]
].rename(columns={"jalaliDate": "date", "name": "symbol"})

#%%
trade_df = pd.read_parquet(PRICE_PATH.format("raw_flat_trade_df.parquet"))
trade_df["date"] = trade_df.date.astype(int)
# %%
sell_df = trade_df[trade_df.nTradeShares < 0]
buy_df = trade_df[trade_df.nTradeShares > 0]
#%%
individual_accounts = sell_df.merge(just_ind_sell_df, on=["symbol", "date"])[
    ["accountId"]
].drop_duplicates()
individual_accounts = (
    individual_accounts.append(
        buy_df.merge(just_ind_buy_df, on=["symbol", "date"])[["accountId"]]
    )
    .drop_duplicates()
    .reset_index(drop=True)
)
individual_accounts
#%%
institutional_accounts = sell_df.merge(just_ins_sell_df, on=["symbol", "date"])[
    ["accountId"]
].drop_duplicates()
institutional_accounts = (
    institutional_accounts.append(
        buy_df.merge(just_ins_buy_df, on=["symbol", "date"])[["accountId"]]
    )
    .drop_duplicates()
    .reset_index(drop=True)
)
institutional_accounts
#%%

#%%
buy_df[
    (buy_df.symbol == "تاصیکو")
    & (buy_df.date == 13980209)
    & (buy_df.nTradeShares <= 4896)
]
#%%
buy_df[
    (buy_df.symbol == "زشریف")
    & (buy_df.date == 13980231)
    & (buy_df.nTradeShares <= 139)
]
#%%
valid_symbols_df = pd.read_parquet(VALID_SYMBOLS_PATH.format("validSymbols.parquet"))
#%%
portfolio_df = pd.read_parquet(PATH_PORTFOLIO.format("portfolio_df.parquet"))
portfolio_df = portfolio_df.merge(valid_symbols_df,on = ['symbol'])
# %%
# one_symbol_portfo_df = portfolio_df.groupby("accountId").filter(lambda x: len(x) < 2)
one_symbol_portfo_df = portfolio_df[
    portfolio_df.groupby("accountId")['accountId'].transform('size') <2
]
# %%
price_df = pd.read_parquet(PRICE_PATH.format("Cleaned_Stock_Prices_14001127.parquet"))
price_df = price_df[price_df.jalaliDate == 13980105][['name','close_price']].rename(columns = {'name':'symbol'})
price_df
one_symbol_portfo_df = one_symbol_portfo_df.merge(price_df,on = ['symbol'])
# %%
one_symbol_portfo_df['account_value']  = one_symbol_portfo_df.nHeldShares * one_symbol_portfo_df.close_price
individual_accounts = individual_accounts.append(
    one_symbol_portfo_df[one_symbol_portfo_df.account_value <= 1e7][['accountId']]
    ).drop_duplicates()
# %%
individual_accounts.to_parquet(PRICE_PATH.format('individual_accounts.parquet'))
institutional_accounts.to_parquet(PRICE_PATH.format('institutional_accounts.parquet'))