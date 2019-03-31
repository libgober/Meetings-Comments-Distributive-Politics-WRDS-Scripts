#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed Oct 11 17:51:18 2017

@author: brianlibgober

This file builds the data necessary to conduct the event study
as well as conducts it.

"""


################################## SETUP ######################################
import os
import pandas as pd
from datetime import datetime,timedelta
from sqlalchemy import create_engine
import numpy as np
import subprocess as sp
import arrow
import apsw
import glob
from time import sleep
os.chdir(os.path.expandvars("$REPFOLDER"))

#%% HELPERS

def block_until_complete():
    result = int(sp.check_output('qstat | wc -l',shell=True))
    while result != 0:
        sleep(10)
        result = int(sp.check_output('qstat | wc -l',shell=True))


def make_series_if_not_exist(symbol,req):
    if os.path.exists("Data/{symbol}.csv".format(symbol=symbol)):
        return
    #otherwise do stuff
    request=req.format(symbol=symbol,month="",day="",starttime="9:35",endtime="16:00")
    with open("{symbol}.sas".format(symbol=symbol),"w+") as f:
        f.write(request)
    sp.call("qsas {symbol}.sas".format(symbol=symbol),
            shell=True)

    
#%% REFERENCE CONSTANTS
request  = """
/* Acquire all minute by minute stock data from {symbol}
as a big CSV that can be imported into a SQL database*/
/* START DATA STEP */
data d1 (rename=(BB=BEST_BID BO=BEST_ASK SYMBOL=SYM_ROOT)) /view=d1;
      /* Define the dataset we will use as source */
      set taq.nbbo_2010{month}{day}: taq.nbbo_2011{month}{day}: taq.nbbo_2012{month}{day}: taq.nbbo_2013{month}{day}:
	(KEEP = Date TIME BB BO SYMBOL );
      /* no need to include irrelevant stuff */
      where
        BB <> 0 and BO <> 0 and SYMBOL='{symbol}'
          and TIME between "{starttime}"t and "{endtime}"t;
      /* add a grouping variable */
      TimeID = dhms(DATE,hour(TIME),minute(TIME),0);
      format TimeID Datetime15.;
      PRICE = ((BB + BO)/2);

data d2 (rename=(BEST_BID=BEST_BID BEST_ASK=BEST_ASK Sym_Root=SYM_ROOT)) /view=d2;
      /* Define the dataset we will use as source */
      set taqmsec.nbbom_2014{month}{day}: taqmsec.nbbom_2015{month}{day}: 
          taqmsec.nbbom_2016{month}{day}: taqmsec.nbbom_2017{month}{day}:
	(KEEP = Date Time_M BEST_BID BEST_ASK Sym_Root SYM_SUFFIX);
      /* no need to include irrelevant stuff */
      where
        BEST_BID <> 0 and BEST_ASK <> 0 and Sym_Root='{symbol}' and
        SYM_SUFFIX='' and Time_M between "{starttime}"t and "{endtime}"t;
      /* add a grouping variable */
      TimeID = dhms(DATE,hour(Time_M),minute(Time_M),0);
      format TimeID Datetime15.;
      PRICE = ((BEST_BID + BEST_ASK)/2);

data d /view=d;
set d1 d2;

 
      /*BEGIN SUMMARIZING      */
PROC MEANS NOPRINT DATA=d mean;
CLASS TimeID /Missing;
/* By default, Proc means looks at all 2^k combinations of k class variables.*/
/*We only want all three, and this command accomplishes that */
Types TimeID;
var PRICE;
/*OUTPUT DATASET NAME */
OUTPUT out=e;
run;


data f (DROP= _STAT_ 
        rename=(TimeID=time Price=quote));
  set e (KEEP = TimeID Price _STAT_);
  where _STAT_="MEAN";
  format TimeID;

PROC EXPORT data=f OUTFILE="Data/{symbol}.csv" DBMS=csv REPLACE;
run;
"""

additional_SAS_commands="""
X "~/anaconda2/bin/python ./Scripts/additional_processing.py {symbol}";
run;
"""

#%% We need these days.

with open("make_stock_days.sas","w+") as f:
    f.write("""
data d (rename=(date=caldt));
set ff.factors_daily (keep=date);
format date yymmdd10.;

PROC EXPORT data=d OUTFILE="Data/stock_days.csv" DBMS=csv REPLACE;
run;
""")

sp.call(["qsas","make_stock_days.sas"])
block_until_complete()


#%% 
#first we must the series for the market funds
make_series_if_not_exist("RSP",request)
make_series_if_not_exist("VTI",request)
block_until_complete()
#file location for listing each stock
loc = os.path.expandvars("$GITFOLDER/symbol_times_to_analyse.csv")
Symbols = pd.read_csv(loc).Symbol.unique()

for symbol in Symbols:
    make_series_if_not_exist(symbol,request+additional_SAS_commands)
block_until_complete()

#to do list
pd.Series(Symbols).to_csv("todo_list.csv",index=0)
sp.call("mkdir -f Analysis",shell=True)
sp.call("rm -f claimed.csv",shell=True)
sp.call("rm -f ~/tmp_lock_file",shell=True)
for i in xrange(1,6):
    #-V switch ensures that the environmental variables are exported
    call="qsub -V -cwd -N analyzer_no{i} -j y -b y " + \
    "'seq 24 | ~/anaconda2/bin/parallel -n0 ~/anaconda2/bin/python Scripts/threadsmart_queue.py'"
    sp.call(call.format(i=i),shell=True)

block_until_complete() 


sp.call("rm -f analysis.sqlite",shell=True)
conn2 = create_engine("sqlite:///analysis.sqlite")
for i in glob.glob("Analysis/*.csv"):
    d = pd.read_csv(i)
    d=d[~d.unixtime.isnull()]
    d.unixtime = d.unixtime.astype('int')
    d.to_sql("main",conn2,if_exists="append",index=False)
