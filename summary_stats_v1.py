#%%
import pandas as pd
import requests
import seaborn as sns
import os
from tqdm import tqdm


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
        {"jalaliDate": jalaliDate, "Value": Value,}, columns=["jalaliDate", "Value"],
    )
    df["jalaliDate"] = df.jalaliDate.apply(removeSlash)
    return df


a = pd.read_csv(r"C:\Users\Administrator\Heidari_Ra\Data\\" + "Index.csv" ).drop(columns = ['Unnamed: 0'])

# PATH_TRADE = "/home/user1/Data/Trade_Data/"
PATH_PORTFOLIO = r"C:\Users\Administrator\Heidari_Ra\Data\\"
# PATH_RESULT = "/home/user1/Result/"
PRICE_PATH = r"C:\Users\Administrator\Heidari_Ra\FinalResult\\"
# VALID_SYMBOLS_PATH = "/home/user1/Data/"

#%%
a = a[(a.jalaliDate > 13980000) & (a.jalaliDate < 13980400)]
portfolio_df = pd.read_parquet(PATH_PORTFOLIO + "{}".format("portfolio_df.parquet"))
#
df = pd.read_parquet(PRICE_PATH + "{}".format("finalResult.parquet"))
print(len(df))
df = df.drop_duplicates()
print(len(df))
df["return"] = df["return"] * 100
df["netCashIn"] = abs(df.netCashIn)
#%%
p = 0.0009
(
    df.netCashOut.quantile(p),
    df.finalPortfolioValue.quantile(p),
    df.netCashIn.quantile(p),
    df.initialPortfolioValue.quantile(p),
)

#%%
result = {}
for i in [
    "netCashOut",
    "netCashIn",
    "initialPortfolioValue",
    "finalPortfolioValue",
    "meanSettlementValue",
    "tradeFrequency",
    "nBuyDays",
    "nSellDays",
    "activeDays",
    "return",
    ]:
    tempt = {}
    if i in  ['return',"initialPortfolioValue",
    "finalPortfolioValue",]:
        tempt["mean"] = df[i].mean()
        tempt['min' ] = df[i].min()
        # tempt["0.1%"] = df[i].quantile(0.001)
        tempt["1%"] = df[i].quantile(0.01)
        tempt["10%"] = df[i].quantile(0.1)
        tempt["median"] = df[i].quantile(0.5)
        tempt["90%"] = df[i].quantile(0.9)
        tempt["99%"] = df[i].quantile(0.99)
        tempt["99.9%"] = df[i].quantile(0.999)
        tempt['max' ] = df[i].max()
    else:
        tempt["mean"] = df[df.tradeFrequency>0][i].mean()
        tempt['min' ] = df[df.tradeFrequency>0][i].min()
        # tempt["0.1%"] = df[df.tradeFrequency>0][i].quantile(0.001)
        tempt["1%"] = df[df.tradeFrequency>0][i].quantile(0.01)
        tempt["10%"] = df[df.tradeFrequency>0][i].quantile(0.1)
        tempt["median"] = df[df.tradeFrequency>0][i].quantile(0.5)
        tempt["90%"] = df[df.tradeFrequency>0][i].quantile(0.9)
        tempt["99%"] = df[df.tradeFrequency>0][i].quantile(0.99)
        tempt["99.9%"] = df[df.tradeFrequency>0][i].quantile(0.999)
        tempt['max' ] = df[df.tradeFrequency>0][i].max()
    result[i] = tempt
pd.DataFrame.from_dict(result).round(2).T
#%%

tempt = df[(
    df.initialPortfolioValue<df.initialPortfolioValue.quantile(0.95)
    )&(
        df.initialPortfolioValue>1
    )]
sns.displot(tempt, x="initialPortfolioValue",kind="kde")
tempt = df[(
    df.initialPortfolioValue<df.initialPortfolioValue.quantile(0.99)
    )&(
        df.isBH ==1
    )]
sns.displot(tempt, x="initialPortfolioValue",kind="kde")

# %%
