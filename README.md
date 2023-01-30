# Trade Data Statistical Analysis

We have Created this repo in order to do the pre-processing, and then processing, of trade data. Trade data contains every transaction in Iran Stock Market from 3/25/2019 to 2/16/2022. We have also used the portfolio data and data gathered by our team. Portfolio data is the snapshot of the portfolios on 3/25/2019. We will merge this dataframe with trade data to create portfolios(daily, monthly). The data gathered by our team is detailed information on securities(price, close price, adjusted close price, minimum price, maximum price, total shares outstanding, market cap and etc) on daily timeframe. 

To do the pre-processing, the procedure is divided to steps. In each notebook, we put one step forward. The notebooks are:

0. initialFunctionsPath

1. protfolioData

2. mergeTradeData

3. validSymbols

4. flattedTradeData

5. portfolio


## 0. initialFunctionsPath

In this notebook we import all the libraries, functions, paths and variables that are used in our processing and pre-processing.


## 1. portfolioData

In this notebook, we clean the portfolio data, including: changing the variable names, aggregating the asstes, removing and changing Arabic characters and removing extra spaces and characters.‌The output of this notebook is a cleaned dataframe of initial portfolio (portfolio on first date of the data)


## 2. mergeTradeData

In this notebook, we clean th trade data, including: changing variable names, replacing missing values with their actual values and removing Arabic characters. The output of this notebbok is a cleaned dataframe of trade data.

## 3. validSymbols

Since there are some securities that are not of our interest, in this notebook we create a dataframe of valid symbols (symbols of our interest). Therefore, we include ETFs, right offers and other valid symbols and exclude invalid symbols from the main dataframe. The output is a dataframe with valid symbols.


## 4. flattedTradeData

Each row of the raw trade data includes both the seller and buyer id. In this notebook, we break the raw dataframe into 2 dataframes; buyer and seller. Then, we union these 2 dataframes to create a dataframe in which each row belongs to 1 id; seller or buyer. The output of this notebook is flatted trade dataframe.

## 5. portfolio

One of the greatest challenges dealing with this data was creating daily and monthly portfolios. Since we wanted to create portfolios according to trade data and
initial portfolio, when adding up total shares, some of the account-symbols became negative. This was, probably, due to the transfers other than trades (for example 
converting right offers to shares). Further, we deal with this challenge differently. For now, we only put a tag on account-symbols that become negative. 
Moreover, we adjust the number of shares in each portfolio.
--
