import pandas as pd
import matplotlib.pyplot as plt

def set_symbol_to_pair(symbol):
    pair = symbol.replace("/","-").upper() # 存檔時不能有 "/"
    return(pair)

filename = './history/{}-{}-data.csv'.format(set_symbol_to_pair("BTC/USD"), '15m') # 存檔的檔名
ohlcv = pd.read_csv(filename,index_col = "time",parse_dates = ["time"])
# print(ohlcv)
close = ohlcv['close'].tail(1000)

sma20 = close.rolling(20).mean()
sma60 = close.rolling(60).mean()

entries = (sma20 > sma60) & (sma20.shift() < sma60.shift())
exits = (sma20 < sma60) & (sma20.shift() > sma60.shift())

close.plot()
sma20.plot()
sma60.plot()
entries.astype(int).plot(secondary_y=True)
(-exits.astype(int)).plot(secondary_y=True)
plt.show()

