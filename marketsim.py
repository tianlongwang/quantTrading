'''
  @author: Tianlong Wang
  Homework 3
'''
from sys import argv
import numpy as np
import QSTK.qstkutil.qsdateutil as du
import QSTK.qstkutil.tsutil as tsu
import QSTK.qstkutil.DataAccess as da
import pandas as pd
import datetime as dt

#TODO: only used in ipython, remove when production
pd.set_option('expand_frame_repr', False)

if __name__ == "__main__":
  try:
    i_start_cash = int(argv[1])
    s_inp_filename = argv[2]
    s_oup_filename = argv[3]
  except:
    print "Usage: python marketsim.py total_cash input_filename output_filename"
    exit(1)
#  fc_marketsim(i_start_cash, s_inp_filename, s_oup_filename)

#def fc_marketsim(i_start_cash, s_inp_filename, s_oup_filename):
  #read csv into "trades"
  ls_header = ['year', 'month', 'day', 'symbol', 'b/s', 'volume']
  df_trades = pd.read_csv(s_inp_filename, index_col=False, names=ls_header)
  ls_symbols = set(df_trades.get('symbol'))
  ls_port_syms = [x.lstrip(' ').rstrip(' ') for x in ls_symbols]
  ls_symbols = ls_port_syms

  #build list of symbols
  #build date boundaries
  na_dates = df_trades.get(['year', 'month', 'day']).values
  ldt_dates = [dt.datetime(int(z[0]),int(z[1]),int(z[2])) for z in na_dates]
  ldt_dates.sort()
  dt_start = ldt_dates[0]
  dt_end = ldt_dates[len(ldt_dates)-1]

  #read data
  #TODO: read only data with dates
  c_dataobj = da.DataAccess('Yahoo', cachestalltime=0)
  ls_all_syms = c_dataobj.get_all_symbols()
  ls_bad_syms = list(set(ls_port_syms) - set(ls_all_syms))

  if len(ls_bad_syms) != 0:
      print "Portfolio contains bad symbols : ", ls_bad_syms

  for s_sym in ls_bad_syms:
      i_index = ls_port_syms.index(s_sym)
      ls_port_syms.pop(i_index)
      lf_port_alloc.pop(i_index)
  dt_timeofday = dt.timedelta(hours=16)
  ldt_timestamps = du.getNYSEdays(dt_start, dt_end+dt.timedelta(days=1), dt_timeofday)

  ls_keys = ['close']

  ldf_data = c_dataobj.get_data(ldt_timestamps, ls_port_syms, ls_keys)
  d_data = dict(zip(ls_keys, ldf_data))
  df_prices = d_data['close'].copy()


  #portfolio holding
  df_initrow = pd.DataFrame(np.zeros([1,len(df_trades.columns)]) ,index=[0], columns=df_trades.columns)
  df_port = pd.concat([df_initrow,df_trades])
  df_port = df_port.reset_index()
  del df_port['index']

  for s in ls_symbols:
    df_port[s] = 0


  for ind in df_port.index:
    if ind == 0:
      continue
    ts_current = df_port.loc[ind, :]
    ts_previous = df_port.loc[ind-1, :]
    symbol = ts_current['symbol']
    for s in ls_symbols:
      if s != symbol:
        df_port[s][ind] = ts_previous[s]
        continue
      if ts_current['b/s'] == 'Buy':
        df_port[symbol][ind] = ts_previous[symbol] + ts_current['volume']
      elif ts_current['b/s'] == 'Sell':
        df_port[symbol][ind] = ts_previous[symbol] - ts_current['volume']
      else:
        raise Exception('invalid b/s')


  #price
  df_port['timestamp'] = 0.

  for s in ls_symbols:
    df_port[s+'_p'] = 0.
  for ind in df_port.index:
    if ind == 0: continue
    i_ymd = df_port.loc[ind, ['year','month', 'day']].values.astype(int)
    dt_ind = dt.datetime(i_ymd[0],i_ymd[1],i_ymd[2])
    dt_next = dt_ind + dt.timedelta(days=1)
    ltp = du.getNYSEdays(dt_ind, dt_next, dt_timeofday)
    if ltp == []: raise Exception('bad order days, NYSE not open')
    tp = ltp[0]
    df_port['timestamp'][ind]=tp
    for s in ls_symbols:
      df_port[s+'_p'][ind] = df_prices[s][tp]

  #cash
  df_port['cash'] = 0
  df_port['cash'][0] = i_start_cash

  for ind in df_port.index:
    if ind == 0: continue
    symbol = df_port['symbol'][ind]
    if df_port['b/s'][ind] == 'Sell':
      df_port['cash'][ind] = df_port['cash'][ind-1] + df_port['volume'][ind] * df_port[symbol+'_p'][ind]
    elif df_port['b/s'][ind] == 'Buy':
      df_port['cash'][ind] = df_port['cash'][ind-1] - df_port['volume'][ind] * df_port[symbol+'_p'][ind]


  #create total value
  df_port['total_value'] = 0

  ls_symbols_p = [x+'_p' for x in ls_symbols]

  for ind in df_port.index:
    na_vol = df_port.loc[ind,ls_symbols].values
    na_pri = df_port.loc[ind,ls_symbols_p].values
    df_port['total_value'][ind] = df_port['cash'][ind] + np.dot(na_vol, na_pri)

  print df_port

  #daily display of total_value
  df_daily = df_port.copy()
  del df_daily['symbol']
  del df_daily['b/s']
  del df_daily['volume']
  del df_daily['year']
  del df_daily['month']
  del df_daily['day']

  df_daily = df_daily.drop_duplicates('timestamp', keep='last')
  df_daily = df_daily.drop(0)

  df_prices.reset_index(inplace=True)
  d_col_rename = dict(zip(ls_symbols,ls_symbols_p))
  d_col_rename['index'] = 'timestamp'
  df_prices.rename(columns=d_col_rename, inplace=True)

  df_merge = pd.concat([df_prices, df_daily])
  df_merge.sort_values('timestamp', inplace=True)
  df_merge.drop_duplicates('timestamp', keep='last', inplace=True)
  df_merge.fillna(method='ffill', inplace=True)
  df_merge.reset_index(inplace=True)
  del df_merge['index']

  for ind in df_merge.index:
    na_vol = df_merge.loc[ind,ls_symbols].values
    na_pri = df_merge.loc[ind,ls_symbols_p].values
    df_merge['total_value'][ind] = df_merge['cash'][ind] + np.dot(na_vol, na_pri)
  df_merge['total_value'] = df_merge['total_value'].astype(int)

  #output
  df_merge['year'] = [tss.to_datetime().year for tss in df_merge['timestamp']]
  df_merge['month'] = [tss.to_datetime().month for tss in df_merge['timestamp']]
  df_merge['day'] = [tss.to_datetime().day for tss in df_merge['timestamp']]

  print df_merge['total_value']

  df_merge.to_csv(s_oup_filename, columns=['year', 'month','day','total_value'], index=False, header=False)
