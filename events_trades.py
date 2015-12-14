'''
@author: Tianlong Wang
'''
import logging

import pandas as pd
import numpy as np
import math
import copy
import QSTK.qstkutil.qsdateutil as du
import datetime as dt
import QSTK.qstkutil.DataAccess as da
import QSTK.qstkutil.tsutil as tsu
import QSTK.qstkstudy.EventProfiler as ep
logging.basicConfig(filename='debug.log', level=logging.DEBUG)

"""
Accepts a list of symbols along with start and end date
Returns the Event Matrix which is a pandas Datamatrix
Event matrix has the following structure :
    |IBM |GOOG|XOM |MSFT| GS | JP |
(d1)|nan |nan | 1  |nan |nan | 1  |
(d2)|nan | 1  |nan |nan |nan |nan |
(d3)| 1  |nan | 1  |nan | 1  |nan |
(d4)|nan |  1 |nan | 1  |nan |nan |
...................................
...................................
Also, d1 = start date
nan = no information about any event.
1 = status bit(positively confirms the event occurence)
"""


def find_events(ls_symbols, d_data, f_price):
    ''' Finding the event dataframe '''
    df_close = d_data['actual_close']
    ts_market = df_close['$SPX']

    logging.debug("Finding Events")

    # Creating an empty dataframe
    df_events = copy.deepcopy(df_close)
    df_events = df_events * np.NAN

    # Time stamps for the event range
    ldt_timestamps = df_close.index

    for s_sym in ls_symbols:
        for i in range(1, len(ldt_timestamps)):
            f_symprice_today = df_close[s_sym].ix[ldt_timestamps[i]]
            f_symprice_yest = df_close[s_sym].ix[ldt_timestamps[i - 1]]
            if f_symprice_today < f_price and f_symprice_yest >= f_price:
                df_events[s_sym].ix[ldt_timestamps[i]] = 1
                print s_sym, ldt_timestamps[i]

    return df_events


def fc_events_into_trades(df_events, s_filename):
    ll_trades = []
    dt_lastday = df_events.index[len(df_events.index)-1]

    for s_k in df_events.keys():
      na_events = df_events[s_k].values
      ts_buy = df_events.index[np.nonzero(na_events==1)]
      na_sellindex = [y+5 for y in np.nonzero(na_events==1)[0]]
      d_max_index = len(df_events.index)-1
      na_sellindex = [ min(z, d_max_index) for z in na_sellindex]
      ts_sell = df_events.index[na_sellindex]
      for dt_buy, dt_sell in zip(ts_buy, ts_sell):
        if dt_sell > dt_lastday:
          dt_sell = dt_lastday
        ll_trades.append([dt_buy.year, dt_buy.month, dt_buy.day, s_k,'Buy', 100, dt_buy])
        ll_trades.append([dt_sell.year, dt_sell.month, dt_sell.day, s_k,'Sell',100, dt_sell])
    df_trades = pd.DataFrame(ll_trades, columns=['year','month','day','symbol','b/s','volume','timestamp'])
    df_trades.sort_values('timestamp',inplace=True)
    print df_trades
    df_trades.to_csv(s_filename, index=False, header=False,columns=['year','month','day','symbol','b/s','volume'])
    return df_trades




if __name__ == '__main__':
    dt_start = dt.datetime(2008, 1, 1)
    dt_end = dt.datetime(2009, 12, 31)
    ldt_timestamps = du.getNYSEdays(dt_start, dt_end, dt.timedelta(hours=16))

    dataobj = da.DataAccess('Yahoo')

    ls_symbols = dataobj.get_symbols_from_list('sp5002012')
    ls_symbols.append('$SPX')
    ls_keys = ['open', 'high', 'low', 'close', 'volume', 'actual_close']
    ldf_data = dataobj.get_data(ldt_timestamps, ls_symbols, ls_keys)
    d_data = dict(zip(ls_keys, ldf_data))

    for s_key in ls_keys:
        d_data[s_key] = d_data[s_key].fillna(method='ffill')
        d_data[s_key] = d_data[s_key].fillna(method='bfill')
        d_data[s_key] = d_data[s_key].fillna(1.0)

    for f_price in np.arange(6.,11.):
        logging.debug(f_price)

        df_events = find_events(ls_symbols, d_data, f_price)
        fc_events_into_trades(df_events, str(f_price) + "_trades.csv")
        #print df_events
        #ep.eventprofiler(df_events, d_data, i_lookback=20, i_lookforward=20, s_filename='MyEventStudy'+str(year)+'_price'+str(f_price)+'.pdf', b_market_neutral=True, b_errorbars=True, s_market_sym='$SPX')
