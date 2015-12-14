'''
  @author: Tianlong Wang
'''
import matplotlib.pyplot as plt
from sys import argv
import numpy as np
import QSTK.qstkutil.qsdateutil as du
import QSTK.qstkutil.tsutil as tsu
import QSTK.qstkutil.DataAccess as da
import pandas as pd
import datetime as dt
c_dataobj = da.DataAccess('Yahoo')


if __name__ == "__main__":
  try:
    s_values= argv[1]
    s_bench_sym = argv[2]
  except:
    print "Usage: python analyze.py  values.csv benchmark_symbole"
    exit(1)
#  fc_analyze(s_values, s_bench_sym)


#def fc_analyze(s_values, s_bench_sym):
  #get date stamps
  df_port = pd.read_csv(s_values, index_col=False, names=['year','month','day','value'])
  na_port = df_port['value'].values.astype('float')
  na_dates = df_port.get(['year', 'month', 'day']).values
  ldt_dates = [dt.datetime(int(z[0]),int(z[1]),int(z[2])) for z in na_dates]
  ldt_dates.sort()
  dt_start = ldt_dates[0]
  dt_end = ldt_dates[len(ldt_dates)-1]
  dt_timeofday = dt.timedelta(hours=16)
  ldt_timestamps = du.getNYSEdays(dt_start, dt_end+dt.timedelta(days=1), dt_timeofday)

  #get bench data
  ldf_data = c_dataobj.get_data(ldt_timestamps, [s_bench_sym], ['close'])
  df_bench = ldf_data[0]

  #calculate Benchmark return
  i_start_money = df_port['value'][0]
  f_volume = i_start_money / df_bench[s_bench_sym][0]
  df_bench['value'] = f_volume * df_bench[s_bench_sym].values
  na_bench = df_bench['value'].values

  #plotting
  plt.clf()
  plt.plot(ldt_timestamps, np.vstack([na_bench, na_port]).T)
  plt.legend(['Benchmark', 'Portfolio Return'])
  plt.xlabel('Date')
  plt.ylabel('Return')
  plt.savefig('marketsim-analysze.pdf', format='pdf')
  #plt.show()


  #print answer
  print 'The final value of the portfolio using the sample file is -- ', df_port[-1:].values
  print

  print 'Details of the Performance of the portfolio :'
  print

  print 'Data Range :  ', ldt_timestamps[0], 'to', ldt_timestamps[len(ldt_timestamps)-1]
  print

  na_portf_rets = na_port.copy().astype(float)
  tsu.returnize0(na_portf_rets);

  f_portf_volatility = np.std(na_portf_rets);
  f_portf_avgret = np.mean(na_portf_rets);
  f_portf_sharpe = (f_portf_avgret / f_portf_volatility) * np.sqrt(250);
  f_portf_totalret = na_port[len(na_port)-1] / float(na_port[0])

  na_bench_rets = na_bench.copy()
  tsu.returnize0(na_bench_rets);

  f_bench_volatility = np.std(na_bench_rets);
  f_bench_avgret = np.mean(na_bench_rets);
  f_bench_sharpe = (f_bench_avgret / f_bench_volatility) * np.sqrt(250);
  f_bench_totalret = na_bench[len(na_bench)-1] / na_bench[0]

  print 'Sharpe Ratio of Fund : ', f_portf_sharpe
  print 'Sharpe Ratio of $', s_bench_sym, ': ', f_bench_sharpe
  print
  print 'Total Return of Fund : ', f_portf_totalret
  print 'Total Return of $', s_bench_sym, ': ', f_bench_totalret
  print
  print 'Standard Deviation of Fund : ' , f_portf_volatility
  print 'Standard Deviation of $', s_bench_sym, ' : ', f_bench_volatility
  print
  print 'Average Daily Return of Fund : ', f_portf_avgret
  print 'Average Daily Return of $', s_bench_sym, ' : ', f_bench_avgret



