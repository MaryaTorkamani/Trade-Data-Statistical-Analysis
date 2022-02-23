#%%
import pandas as pd
import requests
import os
from tqdm import tqdm
from initailFunctionsPath import *
conf = SparkConf()
(conf
.set('spark.driver.memory', '100g')
.set('spark.executer.cores', '58')
.set('spark.shuffle.service.index.cache.size', '3g')
.setAppName('Practice') )
sc = SparkContext.getOrCreate(conf=conf)
spark = SparkSession(sc)
#%%
def removeSlash(row):
    X = row.split("/")
    if len(X[1]) < 2:
        X[1] = "0" + X[1]
    if len(X[2]) < 2:
        X[2] = "0" + X[2]

    return int(X[0] + X[1] + X[2])


def Overall_index():
    url = (
        r"http://www.tsetmc.com/tsev2/chart/data/Index.aspx?i=32097828799138957&t=value"
    )
    r = requests.get(url)
    jalaliDate = []
    Value = []
    for i in r.text.split(";"):
        x = i.split(",")
        jalaliDate.append(x[0])
        Value.append(float(x[1]))
    df = pd.DataFrame(
        {
            "jalaliDate": jalaliDate,
            "Value": Value,
        },
        columns=["jalaliDate", "Value"],
    )
    df["jalaliDate"] = df.jalaliDate.apply(removeSlash)
    return df


# a = Overall_index()

a = pd.read_csv(r"C:\Users\Administrator\Heidari_Ra\Data\\" + "Index.csv" ).drop(columns = ['Unnamed: 0'])
a = a[(a.jalaliDate > MIN_ANALYSIS_DATE) & (a.jalaliDate < MAX_ANALYSIS_DATE)]

path = r"C:\Users\Administrator\Heidari_Ra\Outputs\{}"
pathR = r"C:\Users\Administrator\Heidari_Ra\FinalResult\\"
#%%
r = os.listdir(path.format(""))
new_entrance_df = pd.read_parquet(path.format("new_entrantd_time_series_df.parquet"))
r.remove("new_entrantd_time_series_df.parquet")
"new_entrant_account_ids"
new_entrance_ids = pd.read_parquet(path.format("new_entrant_account_ids.parquet"))
new_entrance_ids["new_accounts"] = 1
new_entrance_ids
mapingdict = dict(zip(list(a.jalaliDate),range(1,len(list(a.jalaliDate))+1)))
new_entrance_ids['firstDate'] =  new_entrance_ids.firstDate.astype(int)
new_entrance_ids['enterance_period'] = new_entrance_ids.firstDate.map(mapingdict)
new_entrance_ids
r.remove("new_entrant_account_ids.parquet")
#%%
one_symbol_df = pd.read_parquet(path.format("symbols_in_one_symbol_portfolios_df.parquet"))
one_symbol_df.head(25)
r.remove("symbols_in_one_symbol_portfolios_df.parquet")
one_symbol_df.head(25)
#%%
cash_time_series = pd.read_parquet(path.format("cash_time_series.parquet")).sort_values(
    by=["type", "date"]
)
r.remove("cash_time_series.parquet")
i = "between10MTand20MT"
cash_in_df = cash_time_series[cash_time_series.type == i].rename(
    columns={
        "netCash": i,
        "nAccounts": "nAccounts_" + i,
    }
)[["date", i, "nAccounts_" + i]]

for i in cash_time_series.type.unique()[1:]:
    tempt = cash_time_series[cash_time_series.type == i].rename(
        columns={
            "netCash": i,
            "nAccounts": "nAccounts_" + i,
        }
    )[["date", i, "nAccounts_" + i]]
    cash_in_df = cash_in_df.merge(
        tempt, on= ['date']
    )
cash_in_df.to_excel(pathR                     + "accountValueCashIn.xlsx",index = False)

#%%
try:
    r.remove("meanNumberOfStocksWithinPortfolio.parquet")
except:
    1 + 2
try:
    r.remove("mass_public_stocks.parquet")
except:
    1 + 2
try:
    r.remove("mergedFile.parquet")
except:
    1 + 2
try:
    r.remove("mean_number_of_stocks_within_portfolio.parquet")
except:
    1 + 2


#%%
df = spark.read.parquet(path.format(r[0])).toPandas()
print(len(df),r[0])
for j in tqdm(r[1:]):
    tempt = spark.read.parquet(path.format(j)).toPandas()
    print(len(tempt),j)
    for i in tempt.columns:
        if (i in df.columns) & (i != "accountId"):
            if (tempt[i].isnull().sum() > df[i].isnull().sum()) | (
                tempt[i].shape[0] < df[i].shape[0]
            ):
                tempt = tempt.drop(columns=[i])
            else:
                df = df.drop(columns=[i])
    df = df.merge(tempt, on=["accountId"], how="outer")
df = df.merge(new_entrance_ids, on=["accountId"], how="outer")
df['new_accounts'] = df.new_accounts.fillna(0)

#%%
list(df)
#%%
blockHolder = spark.read.parquet(path.format(r[0])).toPandas()

turnover_df = spark.read.parquet(path.format('turnover.parquet')).toPandas()
#%%
turnover_df.sort_values(by = ['accountId'])
#%%
blockHolder.sort_values(by = ['accountId'])
#%%
df[df.isBH.isnull()].isnull().sum()


#%%

a








#%%
df['new_accounts_profit'] = df['return']  *700/  df['enterance_period']

def Q(x, numb):
    y = pd.cut(x.rank(), bins=numb, duplicates="drop", labels=False)
    # y = pd.qcut(x.rank(), 55, duplicates="drop", labels=False)
    return y + 1
import numpy as np
# df['new_entrance_return_decile'] = np.nan
df.loc[df.new_accounts == 1,'new_entrance_return_decile'] = Q(df[df.new_accounts == 1]['new_accounts_profit'],10)
df.loc[df.new_accounts == 1,'new_entrance_finalPortfolioValue_decile'] = Q(df[df.new_accounts == 1]['finalPortfolioValue'],10)
df.loc[df.new_accounts == 1,'new_entrance_tradeFrequency_decile'] = Q(df[df.new_accounts == 1]['tradeFrequency'],10)
df.loc[df.new_accounts == 1,'new_entrance_activeDays_decile'] = Q(df[df.new_accounts == 1]['activeDays'],10)
df.loc[df.new_accounts == 1,'new_entrance_absSumTradeValue_decile'] = Q(df[df.new_accounts == 1]['absSumTradeValue'],10)
df.loc[df.new_accounts == 1,'new_entrance_turnover_decile'] = Q(df[df.new_accounts == 1]['turnover'],10)
df.isnull().sum()
#%%
df.groupby('returnDecile')[['return']].describe()

#%%
df.to_parquet(pathR + "{}".format("mergedFile.parquet"), index=False)
t = (a.iloc[-1, -1] - a.iloc[1, -1]) / a.iloc[1, -1]
print(t)
df["market"] = t
df.to_csv(pathR + "{}".format("result.csv"), index=False)

# %%
len(df)