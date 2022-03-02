#%%
import pandas as pd
import requests
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np

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
from initailFunctionsPath import *
conf = SparkConf()
(conf
.set('spark.driver.memory', '100g')
.set('spark.executer.cores', '58')
.set('spark.shuffle.service.index.cache.size', '3g')
.setAppName('Practice') )
sc = SparkContext.getOrCreate(conf=conf)
spark = SparkSession(sc)

a = pd.read_csv(r"C:\Users\Administrator\Heidari_Ra\Data\\" + "Index.csv" ).drop(columns = ['Unnamed: 0'])

# PATH_TRADE = "/home/user1/Data/Trade_Data/"
PATH_PORTFOLIO = r"C:\Users\Administrator\Heidari_Ra\Data\\"
# PATH_RESULT = "/home/user1/Result/"
PRICE_PATH = r"C:\Users\Administrator\Heidari_Ra\FinalResult\\"
# VALID_SYMBOLS_PATH = "/home/user1/Data/"

pathR = r"C:\Users\Administrator\Heidari_Ra\FinalResult\\{}"

path = r"C:\Users\Administrator\Heidari_Ra\FinalResult\{}"
df = pd.read_parquet(path.format('mergedFile.parquet'))
#%%
df["return"] = df["return"] * 100
df["netCashIn"] = abs(df.netCashIn)
# df['initialPortfolioValue'] = df.initialPortfolioValue/1e7
# df['finalPortfolioValue'] = df.finalPortfolioValue/1e7
path_price_data = r"C:\Users\Administrator\Heidari_Ra\Data\{}"
price_df = spark.read.parquet(
    path_price_data.format("Cleaned_Stock_Prices_14001127.parquet")
).toPandas().drop_duplicates()

# %%
print(df[df.initialPortfolioValue > 0].shape[0],
 df[df.finalPortfolioValue > 0].shape[0])
df[df.tradeFrequency > 0].shape[0]
#%%
def summary_account(df, columns):
    result = {}
    for i in columns:
        tempt = {}
        tempt["mean"] = df[i].mean()
        # tempt["min"] = df[i].min()
        # tempt["0.1%"] = df[i].quantile(0.001)
        # tempt["1%"] = df[i].quantile(0.01)
        tempt["10%"] = df[i].quantile(0.1)
        tempt["median"] = df[i].quantile(0.5)
        tempt["75%"] = df[i].quantile(0.75)
        tempt["90%"] = df[i].quantile(0.9)
        tempt["95%"] = df[i].quantile(0.95)
        tempt["99%"] = df[i].quantile(0.99)
        tempt["99.9%"] = df[i].quantile(0.999)
        # tempt["max"] = df[i].max()
        result[i] = tempt
    return pd.DataFrame.from_dict(result).round(2)


df["averagePortfolioValue"] = df.initialPortfolioValue + df.finalPortfolioValue / 2
result_account_level = summary_account(
    df, ["initialPortfolioValue", "finalPortfolioValue", "averagePortfolioValue"]
)
result_account_level_new_accounts = summary_account(
    df[df.new_accounts == 1], ["finalPortfolioValue"]
)
result_account_level.merge(result_account_level_new_accounts,
right_index= True,
left_index= True,
suffixes = ("","New_Enterance"))

# %%
tempt = df[['nStocksWithinInitialPortfolio']]

tempt['number_of_stocks'] = '1'
tempt.loc[tempt.nStocksWithinInitialPortfolio >1,'number_of_stocks'] = '2-5'
tempt.loc[tempt.nStocksWithinInitialPortfolio >5,'number_of_stocks'] = ">5"


tempt = tempt.groupby('number_of_stocks').count() / len(df)
data = list(tempt.nStocksWithinInitialPortfolio)
label = list(tempt.index)
plt.pie(data, labels=None, autopct='%1.1f%%', explode=[0.1,0,0], shadow=False, startangle=90)
plt.legend(bbox_to_anchor=(0.9,0.5), loc="lower right", 
                          bbox_transform=plt.gcf().transFigure,labels=label)
plt.title("Number of stocks in portfolio")
plt.savefig(pathR.format("nStocksWithinInitialPortfolio.png"), bbox_inches="tight",dpi = 500)

#%%
tempt = df[df.nStocksWithinInitialPortfolio>1][['nStocksWithinInitialPortfolio']]

tempt['number_of_stocks'] = '2'
tempt.loc[tempt.nStocksWithinInitialPortfolio >2,'number_of_stocks'] = '3'
tempt.loc[tempt.nStocksWithinInitialPortfolio >3,'number_of_stocks'] = '4'
tempt.loc[tempt.nStocksWithinInitialPortfolio >4,'number_of_stocks'] = '5'
tempt.loc[tempt.nStocksWithinInitialPortfolio >5,'number_of_stocks'] = '6-10'
tempt.loc[tempt.nStocksWithinInitialPortfolio >10,'number_of_stocks'] = "10>"


tempt = tempt.groupby('number_of_stocks').count() / len(df[df.nStocksWithinInitialPortfolio>1])
tempt = tempt.T
tempt = tempt[['2', '3', '4', '5', '6-10','10>', ]].T
data = list(tempt.nStocksWithinInitialPortfolio)
label = list(tempt.index)
plt.pie(data, labels=None, autopct='%1.1f%%', explode=[0.1,0.1,0.1,0.1,0.1,0.1], shadow=False, startangle=0)
plt.legend(bbox_to_anchor=(0.9,0.5), loc="lower right", 
                          bbox_transform=plt.gcf().transFigure,labels=label)
plt.title("Number of stocks in portfolio")
plt.savefig(pathR.format("nStocksWithinInitialPortfolio_without_one.png"), bbox_inches="tight",dpi = 500)


#%%
tempt = df[['tradeFrequency']].fillna(0)

tempt['Traded'] = 0
tempt.loc[tempt.tradeFrequency >0,'Traded'] = 1
tempt = tempt.groupby('Traded').count() / len(df)
data = list(tempt.tradeFrequency)
label = [ 'No trade','With Trade',]
plt.pie(data, labels=None, autopct='%1.1f%%', explode=[0.1,0], shadow=False, startangle=0)
plt.legend(bbox_to_anchor=(0.95,0.55), loc="lower right", 
                          bbox_transform=plt.gcf().transFigure,labels=label)
# plt.title("Traded portfolio")
plt.savefig(pathR.format("tradeFrequency.png"), bbox_inches="tight",dpi = 500)



#%%
# from matplotlib import rc

# rc("font", **{"family": "sans-serif", "sans-serif": ["Helvetica"]})
# ## for Palatino and other serif fonts use:
# # rc('font',**{'family':'serif','serif':['Palatino']})
# rc("text", usetex=True)

for i in [0.01, 0.1, 0.25]:
    tempt = df[
        (df.finalPortfolioValue < df[(df.isBH != 1)&(df.new_accounts <1)].finalPortfolioValue.quantile(1 - i))
        & (df.isBH != 1)&(df.new_accounts <1)
    ]
    tempt["finalPortfolioValue"] = tempt.finalPortfolioValue * 1e3
    fig = plt.figure(figsize=(8, 4))
    sns.displot(tempt, x="finalPortfolioValue", kind="kde", cut=0)
    # plt.title(
    #     r"Distribution of final portfolio for non-block-holders"
    #     r" \small{}".format(
    #         "( droped {} \% of right tail ) ".format(str(int((i) * 100)))
    #     )
    # )
    plt.margins(0)
    plt.xlabel("Thousand tomans")
    current_values = plt.gca().get_xticks()
    plt.gca().set_xticklabels(["{:,.0f}".format(x) for x in current_values])
    plt.savefig(
        pathR.format("portfolioDistribution_{}.png".format(str(i))),
        dpi=1000,
        bbox_inches="tight",
    )
for i in [0.01, 0.1, 0.25]:
    tempt = df[
        (df.finalPortfolioValue < df[(df.isBH != 1)&(df.new_accounts >0)].finalPortfolioValue.quantile(1 - i))
        & (df.isBH != 1)&(df.new_accounts >0)
    ]
    tempt["finalPortfolioValue"] = tempt.finalPortfolioValue * 1e3
    fig = plt.figure(figsize=(8, 4))
    sns.displot(tempt, x="finalPortfolioValue", kind="kde", cut=0)
    # plt.title(
    #     r"Distribution of final portfolio for new non-block-holders"
    #     r" \small{}".format(
    #         "( droped {} \% of right tail ) ".format(str(int((i) * 100)))
    #     )
    # )
    plt.margins(0)
    plt.xlabel("Thousand tomans")
    current_values = plt.gca().get_xticks()
    plt.gca().set_xticklabels(["{:,.0f}".format(x) for x in current_values])
    plt.savefig(
        pathR.format("portfolioDistribution_{}_new.png".format(str(i))),
        dpi=1000,
        bbox_inches="tight",
    )
for i in [0.01, 0.1]:
    tempt = df[
            (df.finalPortfolioValue < df[(df.isBH != 1)&(df.new_accounts <1)].finalPortfolioValue.quantile(1 - i))
            & (df.isBH != 1)&(df.new_accounts <1)
        ]
    # tempt["finalPortfolioValue"] = tempt.finalPortfolioValue * 1e3
    fig = plt.figure(figsize=(8, 4))
    sns.ecdfplot(tempt, x="finalPortfolioValue")
    # plt.title(
    #         r"Distribution of final portfolio for non-block-holders"
    #         r" \small{}".format(
    #             "( droped {} \% of right tail ) ".format(str(int((i) * 100)))
    #         )
    #     )
    plt.margins(0)
    plt.xlabel("Million tomans")
    current_values = plt.gca().get_xticks()
    plt.gca().set_xticklabels(["{:,.0f}".format(x) for x in current_values])
    current_values = plt.gca().get_yticks()
    plt.gca().set_yticklabels(["{:,.0%}".format(x) for x in current_values])
    plt.savefig(
            pathR.format("portfolioDistribution_{}_cumulative.png".format(str(i))),
            dpi=1000,
            bbox_inches="tight",
        )
    
#%%
# Block-holder
for i in [0.01, 0.1, 0.25]:
    tempt = df[
        (df.finalPortfolioValue < df[(df.isBH == 1)].finalPortfolioValue.quantile(1 - i))
        & (df.isBH == 1)
    ]
    tempt["finalPortfolioValue"] = tempt.finalPortfolioValue / 1e3
    fig = plt.figure(figsize=(8, 4))
    sns.displot(tempt, x="finalPortfolioValue", kind="kde", cut=0)
    plt.title(
        r"Distribution of final portfolio of block-holder"
        r" \small{}".format(
            "( droped {} \% of right tail ) ".format(str(int((i) * 100)))
        )
    )
    plt.xlabel("trillion tomans")
    current_values = plt.gca().get_xticks()
    plt.gca().set_xticklabels(["{:,.0f}".format(x) for x in current_values])
    plt.savefig(
        pathR.format("portfolioBlockholderDistribution_{}.png".format(str(i))),
        dpi=1000,
        bbox_inches="tight",
    )
#%%
df[
        (df.finalPortfolioValue < df.finalPortfolioValue.quantile(1 - i))
        & (df.isBH == 1)
        ].sort_values(by = ['finalPortfolioValue'])[['finalPortfolioValue']]

#%%

for i in [0.05, 0.10, 0.25]:
    t1 = df[
        (df.initialPortfolioValue < df.initialPortfolioValue.quantile(1 - i))
    ].rename(columns={"initialPortfolioValue": "portfolioValue"})
    t1["portfo"] = "Initial Portfolio"
    t2 = df[(df.finalPortfolioValue < df.finalPortfolioValue.quantile(1 - i))].rename(
        columns={"finalPortfolioValue": "portfolioValue"}
    )
    t2["portfo"] = "Final Portfolio"
    tempt = pd.concat(
        [t1[["portfolioValue", "portfo"]], t2[["portfolioValue", "portfo"]]]
    ).reset_index(drop=True)
    tempt
    tempt["portfolioValue"] = tempt.portfolioValue * 1e3
    g = sns.displot(tempt, x="portfolioValue", hue="portfo", kind="kde", cut=0)
    plt.title(
        r"Distribution of initial and final portfolio"
        r" \small{}".format(
            "( droped {} \% of right tail ) ".format(str(int((i) * 100)))
        )
    )
    plt.xlabel("Thousand tomans")
    current_values = plt.gca().get_xticks()
    plt.gca().set_xticklabels(["{:,.0f}".format(x) for x in current_values])
    plt.savefig(
        pathR.format("portfolioBeforeAfterDistribution_{}.png".format(str(i))),
        dpi=1000,
        bbox_inches="tight",
    )

# %%
price_df = price_df[(price_df.jalaliDate > 13980000) & (price_df.jalaliDate < 14001114)]
tempt = price_df.groupby("jalaliDate")[["MarketCap"]].sum()
r = pd.concat([tempt.head(1), tempt.tail(1)]).T.rename(
    columns={13980105: "initialPortfolioValue", 14001113: "finalPortfolioValue"}
)
r = r / 1e7
t = df[["initialPortfolioValue", "finalPortfolioValue"]].sum().to_frame().T
pd.concat([r, t]).T.rename(columns={0: "portfolioValues"}).T
#%%
def summary_account(df, columns):
    result = {}
    for i in columns:
        tempt = {}
        tempt["mean"] = df[i].mean()
        tempt["median"] = df[i].quantile(0.5)
        # tempt["min"] = df[i].min()
        # tempt["0.1%"] = df[i].quantile(0.001)
        # tempt["1%"] = df[i].quantile(0.01)
        tempt["10%"] = df[i].quantile(0.1)
        tempt["75%"] = df[i].quantile(0.75)
        tempt["90%"] = df[i].quantile(0.9)
        tempt["99%"] = df[i].quantile(0.99)
        tempt["99.9%"] = df[i].quantile(0.999)
        # tempt["max"] = df[i].max()
        result[i] = tempt
    return pd.DataFrame.from_dict(result).astype(int)
summary_account(df, [
    'nStocksWithinInitialPortfolio','nStocksWithinFinalPortfolio'
    ])


#%%
df = pd.read_parquet(path.format("mergedFile.parquet"))
# df["averagePortfolioValue"] = df.initialPortfolioValue + df.finalPortfolioValue / 2
# df['sumTradeValue'] = abs(df.sumTradeValue)

def trade_summary(df, columns):
    result = {}
    for i in columns:
        tempt = {}
        tempt["mean"] = df[i].mean()
        tempt["min"] = df[i].min()
        # tempt["0.1%"] = df[i].quantile(0.001)
        # tempt["1%"] = df[i].quantile(0.01)
        tempt["10%"] = df[i].quantile(0.1)
        tempt["median"] = df[i].quantile(0.5)
        tempt["90%"] = df[i].quantile(0.9)
        tempt["99%"] = df[i].quantile(0.99)
        tempt["99.9%"] = df[i].quantile(0.999)
        # tempt["max"] = df[i].max()
        result[i] = tempt
    return pd.DataFrame.from_dict(result).round(2).T


df["nBuyDays"] = df.nBuyDays / len(tempt)
df["nSellDays"] = df.nSellDays / len(tempt)
df["activeDays"] = df.activeDays / len(tempt)
len(tempt)
columns = [
    "tradeFrequency",
    "absSumTradeValue",
    "turnover",
    "activeDays",
    "nBuyDays",
    "nSellDays",
    "return",
]


trade_summary(df, columns)


# %%
one_symbol_df = pd.read_parquet(path.format("symbols_in_one_symbol_portfolios_df.parquet"))
one_symbol_df['percent'] = one_symbol_df.accountNumbers / one_symbol_df.accountNumbers.sum() *100
one_symbol_df.head(20)
# %%
