import QSTK.qstkutil.qsdateutil as du
import QSTK.qstkutil.tsutil as tsu
import QSTK.qstkutil.DataAccess as da
import datetime as dt
import matplotlib.pyplot as plt
import pandas as pd

#
# Prepare to read the data
#
ls_symbols = ["AAPL","MSFT"]
dt_start = dt.datetime(2010,4,15)
dt_end = dt.datetime(2010,6,30)
dt_timeofday=dt.timedelta(hours=16)
ldt_timestamps = du.getNYSEdays(dt_start,dt_end,dt_timeofday)

c_dataobj = da.DataAccess('Yahoo')
df_close = c_dataobj.get_data(ldt_timestamps, ls_symbols, "close")

df_close = df_close.fillna(method='ffill')
df_close = df_close.fillna(method='bfill')

df_means = pd.rolling_mean(df_close,20,min_periods=20)
df_std = pd.rolling_std(df_close,20,min_periods=20)
df_upper_band = df_means + df_std
df_lower_band = df_means - df_std
df_boll_val = (df_close - df_means )/ df_std
print df_boll_val

# Plot the prices
plt.clf()

s_symtoplot = 'AAPL'
plt.plot(df_close.index,df_close[s_symtoplot].values,label=s_symtoplot)
plt.plot(df_close.index,df_means[s_symtoplot].values)
plt.plot(df_upper_band.index,df_upper_band[s_symtoplot].values)
plt.plot(df_lower_band.index,df_lower_band[s_symtoplot].values)
plt.legend([s_symtoplot,'Moving Avg.', 'upper', 'lower'])
plt.ylabel('Adjusted Close')
plt.show()

plt.savefig("movingavg-ex.png", format='png')
