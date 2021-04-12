import csv

import yfinance as yf

tickers_name=''
with open('bats_symbols.csv', newline='') as csvfile:
    reader = csv.reader(csvfile, delimiter=',')
    next(reader)
    for row in reader:
        tickers_name += ' '+row[0]


tickers = yf.Tickers(tickers_name)
for ticker in tickers.tickers:
    if len(ticker.major_holders)==4:
        print(ticker.ticker)
        print(ticker.major_holders)