#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 17 10:53:47 2017

SAS is great for what it does well: grabbing and summarizing a lot of data fast.
But in many ways it's not a modern tool. For prototyping and development
it's way behind other languages like R or Python, and for exports it's not great.
This script takes the .csv produced by SAS and
     * puts all series on a homogenous time scale,
     * back fills missing prices (no price interpreted as meaning no change)
     * residualizes the differences against two market models
     * reintegrates the series
     * exports to a sqlite database.

Indices are added to the db and it is tightly compressed using lzma.

Tested on EBTC.csv

This file is typically called by SAS. To avoid environmental uncertainties,
just be sure that the script is invoked in the right directory.

@author: brianlibgober
"""

#additional processing of script
import sys
import pandas as pd
from datetime import date
import numpy as np
from sqlalchemy import types, create_engine
import statsmodels.api as sm
import os
import subprocess as sp
#%%
#the name of the stock
sym = sys.argv[1]
#need to read more than one csv, so we won't format this right away
inloc = "Data/{sym}.csv"
#we only use this once,ok to format now
engine = create_engine(os.path.expandvars(
        "sqlite:///Data/{sym}.sqlite".format(sym=sym)))

print sym
#load the reference list of stock days
days = pd.read_csv("Data/stock_days.csv")
#subset to after the 2010 period
days = days[(days.caldt >= '2010-01-01') & (days.caldt <='2016-11-23')]
days.caldt = days.caldt.astype('datetime64[D]')
#we will want to cross join with the minutes between 9:35 and 16:60
mins = pd.DataFrame({"mins" : range(9*60+35,16*60+1,1)})
#do the cross join, a little silly that this is the recommended way on StackOverflow
days["key"] =1
mins["key"] = 1
full = pd.merge(days,mins,on="key")
#
alltimes = pd.DataFrame({"time" :
    pd.to_datetime(full.caldt,utc=True) + pd.to_timedelta(full.mins,unit='m')
    })

alltimes["time"] = alltimes.time.values.astype('datetime64[s]')
    
    
def join_series(sym,data):
    """
    Joins the series in csv located at loc to the data file
    """
    #load the data for this particular stock
    d = pd.read_csv(inloc.format(sym=sym),index_col=False)
    d.columns = ["time",sym]
    #correct for SAS time
    d.time=d.time-315619200
    #make sure datetime conforms
    d.index = d.time.astype('datetime64[s]')
    d.drop('time',1,inplace=True)
    #merge
    data2=data.join(d,on="time")
    return data2


d = join_series('VTI',alltimes)
d = join_series("RSP",d)
d = join_series(sym,d)
d.columns = ["time","VTI","RSP","quote"]


def fillPrices(x):
    """
    We want to fill sym=EBTF => d.ix[4:7,:]

                    time        VTI        RSP   quote
    4 2010-01-04 09:39:00  57.018023  39.965000  11.065
    5 2010-01-04 09:40:00  57.056458  39.977742     NaN
    6 2010-01-04 09:41:00  57.058721  39.969306     NaN
    7 2010-01-04 09:42:00  57.040952  39.957692  11.070

    Generally speaking the best guess for nan means no change.
    If we ignore all these na's, after we difference we lose that 11.07 datapoint.
    """
    out = []
    lastvalue = np.nan
    for i in x:
        #e.g. short for if not np.nan
        if i == i:
            lastvalue = i
            out.append(i)
        else:
            out.append(lastvalue)
    return out

d[["VTI","RSP","quote"]] = d[["VTI","RSP","quote"]].apply(fillPrices)


#calculate the returns
d["VTI_R"] = d.VTI.diff()
d["RSP_R"] = d.RSP.diff()
d["R"] = d.quote.diff()
#calculate the market residuals
def residualize(response,predictor):
    Y = d[response]
    X = d[predictor]
    include = (~Y.isnull()) & (~X.isnull())
    X=X[include]
    Y=Y[include]
    X=sm.add_constant(X)
    est = sm.OLS(Y, X)
    est = est.fit()
    print "Beta", est.params[1]
    return est.resid

d["RETURNS_NET_TOTAL_MARKET"]=residualize("R","VTI_R")
d["RETURNS_NET_SP500_EW"]=residualize("R","RSP_R")
d.drop(["VTI_R","RSP_R","VTI","RSP"],axis=1,inplace=True)

#%% For speed we will calculate quantiles in SQLite
##
d["PATH_SP500_EW"] = d.RETURNS_NET_SP500_EW.cumsum()
d["PATH_TOTALMARKET"] = d.RETURNS_NET_TOTAL_MARKET.cumsum()
d.drop(["RETURNS_NET_TOTAL_MARKET","RETURNS_NET_SP500_EW","R"],axis=1,inplace=True)
#let's be careful about storing datetimes consistently and efficiently
#numpy stores timestamps in nanoseconds,but sqlite uses seconds
d.time = d.time.values.astype("int") // 10**9
d.to_sql("mp",engine,index=False,dtype={"time" : types.INTEGER,
                             "quote": types.FLOAT,
                             "PATH_SP500_EW" : types.FLOAT,
                             "PATH_TOTALMARKET" : types.FLOAT},
    if_exists="replace")

#%%
engine.execute("CREATE UNIQUE INDEX time_idx on mp(datetime(time,'unixepoch'))")
engine.execute("CREATE UNIQUE INDEX time_numeric_idx on mp(time)")
sp.call("~/anaconda2/bin/lzma -efz ./Data/{sym}.sqlite".format(sym=sym),shell=True)
#%% zip it up
