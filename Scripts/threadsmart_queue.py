#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 12 17:41:24 2017

Uses a locking mechanism
http://fasteners.readthedocs.io/en/latest/examples.html

@author: brianlibgober
"""

import os
import pandas as pd
import fasteners
import apsw
import subprocess as sp

#folder where the extracted databases should go
#%%
#for each stock this is the parameter combination we will want
todo = pd.read_csv("todo_list.csv",names=["stock"])
symbol_times_to_analyse = pd.read_csv("symbol_times_to_analyse.csv",index_col=0)

MAINQUERY = \
"""
--example uses 1281445200
with initialprices as (
	select 
		time as t0,
		quote as p01,
		PATH_SP500_EW p02,
		PATH_TOTALMARKET p03 
	from mp 
	where 
	time(time,'unixepoch')=time(:trademinstart,'unixepoch') AND
	time<=:trademinstart
	order by t0 desc
	limit :count
	)
/*select * from initialprices limit 3;
t0         |p01                |p02                    |p03                   |
-----------|-------------------|-----------------------|----------------------|
1281445200 |4.015              |-3.2693789519492107    |-1.3806705609552956   |
1281358800 |4.065              |-3.3363619819013293    |-1.6148377900283513   |
1281099600 |4.035              |-3.1442862480379006    |-1.1992866480679423   |
*/
,finalprices as (
	select time as t1,
		quote as p11,
		PATH_SP500_EW p12,
		PATH_TOTALMARKET p13 from 
		mp where 
		time(time,'unixepoch')=time(:trademinstart+:duration,'unixepoch') AND
		time<=:trademinstart + :duration
		order by t1 desc
		limit :count
)
/*select * from finalprices limit 3;
t1         |p11   |p12                 |p13                 |
-----------|------|--------------------|--------------------|
1281448800 |4.015 |-3.2588199511125127 |-1.3534458650626484 |
1281362400 |4.095 |-3.312884649782538  |-1.608183308478045  |
1281103200 |4.035 |-3.1468922225827107 |-1.1913618452937664 |
*/
,differences as 
(select 
	datetime(t0,'unixepoch') as t0,
	datetime(t1,'unixepoch') as t1,
	p11-p01 as d1,
	p12-p02 as d2,
	p13 - p03 as d3
from initialprices A
left join finalprices B
on date(A.t0,'unixepoch')=date(B.t1,'unixepoch')
)
/*select * from differences limit 3;
t0                  |t1                  |d1                  |d2                     |d3                   |
--------------------|--------------------|--------------------|-----------------------|---------------------|
2010-08-10 13:00:00 |2010-08-10 14:00:00 |0.0                 |0.01055900083669803    |0.0272246958926472   |
2010-08-09 13:00:00 |2010-08-09 14:00:00 |0.02999999999999936 |0.023477332118791328   |0.006654481550306235 |
2010-08-06 13:00:00 |2010-08-06 14:00:00 |0.0                 |-0.0026059745448101523 |0.007924802774175843 |
*/
,comparisons as (select A.t0,A.t1,A.d1,A.d2,A.d3, B.d1 as e1,B.d2 as e2,B.d3 as e3 from differences A
cross join (select * from differences where datetime(:trademinstart,'unixepoch')=t0) B 
limit -1 offset 1)
/*select * from comparisons limit 3;
--e is the event day difference in returns
0                  |t1                  |d1                   |d2                     |d3                   |e1  |e2                  |e3          
-------------------|--------------------|---------------------|-----------------------|---------------------|----|--------------------|------------
010-08-09 13:00:00 |2010-08-09 14:00:00 |0.02999999999999936  |0.023477332118791328   |0.006654481550306235 |0.0 |0.01055900083669803 |0.0272246958
010-08-06 13:00:00 |2010-08-06 14:00:00 |0.0                  |-0.0026059745448101523 |0.007924802774175843 |0.0 |0.01055900083669803 |0.0272246958
010-08-05 13:00:00 |2010-08-05 14:00:00 |0.009999999999999787 |-0.013655092018214798  |-0.04364792551142771 |0.0 |0.01055900083669803 |0.0272246958
*/
,wideresults as (select 
     :sym_root as sym_root,
     date(:trademinstart,'unixepoch') as eventdate,
     time(:trademinstart,'unixepoch') as eventtime,
     :trademinstart as unixtime,
     :duration as duration,
	(avg(d1<=e1)+avg(d1<e1))/(2) as q1,
	sum(d1 is not NULL) as n1,
	avg(d1) as m1,
	stdev(d1) as s1,
	e1 as r1,  --e1 all identical so this is ok
	(e1-avg(d1))/stdev(d1) as t1,
	(avg(d2<=e2)+avg(d2<e2))/(2) as q2,
	sum(d2 is not NULL) as n2,
	avg(d2) as m2,
	stdev(d2) as s2,
	e2 as r2,  --e2 all identical so this is ok
	(e2-avg(d2))/stdev(d2) as t2,
	(avg(d3<=e3)+avg(d3<e3))/(2) as q3,
	sum(d3 is not NULL) as n3,
	avg(d3) as m3,
	stdev(d3) as s3,
	e3 as r3,  --e3 all identical so this is ok
	(e3-avg(d3))/stdev(d3) as t3 from comparisons)
/*select * from wideresults;
q1                 |n1  |m1                     |s1                   |r1  |t1                  |q2                 |n2  |m2                    |s2                  |r2                  |t2                 |q3                 |n3  |m3                     |s3                  |r3                 |t3                 |
-------------------|----|-----------------------|---------------------|----|--------------------|-------------------|----|----------------------|--------------------|--------------------|-------------------|-------------------|----|-----------------------|--------------------|-------------------|-------------------|
0.5132450331125827 |151 |-0.0010815996496688787 |0.022712396328976845 |0.0 |0.04762155582363435 |0.7284768211920529 |151 |-0.006077189104862718 |0.03367658595720302 |0.01055900083669803 |0.4939987076689544 |0.4724061810154525 |151 |-0.0059960648848058394 |0.08310222008047566 |0.0272246958926472 |0.3997578012390315 |
*/
-- now output long
--No market model
select * from wideresults;
"""

def refresh_claims():
    """
    Get a new list of what's been claimed
    """
    try:
        claimed = pd.read_csv("claimed.csv")
    except:
        claimed = pd.DataFrame([],columns=['stock'])
    return claimed

def check_if_any_tasks(todo,claimed):
    """
    Checks whether there is anything on the to do list that hasn't been claimed
    """
    return any(~todo.stock.isin(claimed.stock))

def process_rawdata(rawdata):
    """
    Turns out that going from wide to long with sqlite temporary views
    is pretty slow. Substantial time savings obtained via doing this transform
    in python.
    """
    common = pd.DataFrame(rawdata).iloc[:,0:5]
    common.columns = ["sym_root","eventdate","eventtime","unixtime",'duration']
    #first tmp is the simple returns
    simple=pd.DataFrame(rawdata).iloc[:,5:11]
    simple.columns = ['q','n','m','s','r','t']
    simple = pd.concat([common,simple],axis=1)
    simple["market_model"] = "None"
    #
    rsp=pd.DataFrame(rawdata).iloc[:,11:17]
    rsp.columns = ['q','n','m','s','r','t']
    rsp = pd.concat([common,rsp],axis=1)
    rsp["market_model"] = 'RSP'
    #vti
    vti = pd.DataFrame(rawdata).iloc[:,17:23]
    vti.columns =  ['q','n','m','s','r','t']
    vti = pd.concat([common,vti],axis=1)
    vti["market_model"] = 'VTI'
    out = pd.concat([simple,rsp,vti])
    return out

#%%
claimed = refresh_claims()
a_lock = fasteners.InterProcessLock(os.path.expanduser('~/tmp_lock_file'))
#proceed on assumption that there is something to do
while check_if_any_tasks(todo,claimed):
    ##############  MANAGING MULTIPLE THREADS #####################
    #should cause program to wait until it can acquire the lock
    gotten = a_lock.acquire()
    print "Lock obtained, we will have the most current claims file"
    claimed = refresh_claims()
    #if having obtained the lock and read the most current version
    #see there is nothing to do, we can release the lock and end
    if not check_if_any_tasks(todo,claimed):
        a_lock.release()
        break
    #otherwise claim the first stock
    todo_current = todo.stock[~todo.stock.isin(claimed.stock)]
    stock = todo_current[min(todo_current.index)]
    #update the claimed file
    claimed = claimed.append({"stock" :stock},ignore_index=True)
    claimed.to_csv("claimed.csv",index=False)
    a_lock.release()
    print "Lock released. Succesfully claimed", stock 
    ##############  LOADING IN MEMORY SQLITE DB    ################# 
    lzma_db_filename =  "./Data/{stock}.sqlite.lzma".format(stock=stock)
    db_filename = "./Data/{stock}.sqlite".format(stock=stock)
    analysis_filename = "./Analysis/{stock}.csv".format(stock=stock)
    #sometimes a sqlite file is not generated, these must be
    #separately investigated
    #ideally they would be dropped from sample space or rectified
    if not os.path.exists(lzma_db_filename):
        continue
    #unzip the sqlite file
    sp.call(["lzma","-dk"],stdin=open(lzma_db_filename),stdout=open(db_filename,"w+"))
    print "Uncomprresed sqlite file"
    #we will load the sqlite file into memory for even greater performance 
    conn = apsw.Connection(":memory:")
    disk_conn = apsw.Connection(db_filename)
    # Copy the disk database into memory
    print "Loading db into memory"
    with conn.backup("main", disk_conn, "main") as backup:
        backup.step() # copy whole database in one go
    print"Done! Cleaning up..."
    #the database is in memory, so the disk files we created can go now
    del disk_conn
    sp.call(["rm",db_filename])    
    ##############  OPERATIONS ON  DB             ################# 
    conn.enableloadextension(True)
    conn.loadextension(os.path.expanduser("~/libsqlitefunctions.so"))
    #build a list of parameters to supply to the query
    PARAMS = []
    minutes = range(1,181,1) + range(-90,0,1)
    relevant_times = symbol_times_to_analyse.earliest_time[\
            symbol_times_to_analyse.Symbol==stock]
    for i in relevant_times.unique():
        for m in minutes:
            PARAMS.append(
                    {"trademinstart":i,
                     "duration" : 60*m,
                     "sym_root" : stock,
                     "count" : 200 #No. of comparison days
                     })    
    #add the placebo parameters
    c =conn.cursor()
    print "Starting analysis"
    rawdata = c.executemany(MAINQUERY, PARAMS).fetchall()
    #make the data long
    #SQLite has bad optimizations for making long, this will be much faster
    alldata=process_rawdata(rawdata)
    #output
    alldata.to_csv(analysis_filename,index=False)
    print "Done! Grabbing next stock"
    
print "Nothing left to do! Exiting..."
