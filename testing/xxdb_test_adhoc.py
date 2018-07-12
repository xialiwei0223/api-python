import sys
sys.path.append('..')
from dolphindb import *
import numpy as np
import pandas as pd

from xxdb_server import HOST,PORT

xx = session()
xx.connect(HOST, PORT)
# dt=xx.table("t3")
# print dt.select('id').where(dt.v>5).toDF()

t1 = xx.table(data="quotes", dbPath="dfs://TAQ")
print t1.select("count(symbol)").where("date>2007.09.27").toDF()
# print xx.run('["FB", "GOOG", ] find "GOOG"')
# matrix,rowLabels, colLabels =  xx.run('1..3 +:A 1..6$3:2')
# print matrix.shape
# print matrix[:,0]
# print rowLabels
# print colLabels
# print xx.run('accumulate(add, 7.8f 2.4f NULL -5.6f 11.5f)')
# rs = xx.run("asin:E -1.5f 12f 0.75f")
# print type(rs)
# print rs
# for x in rs:
#     print type(x)
#print xx.run("[nanotimestamp(now()), nanotimestamp(now())]")

# print "Test NanoTimestamp scalar read/upload/read"
# timeStart = datetime.now()
# nanoTimeStamp = xx.run('2012.10.01T15:00:04.552000000')
# #xx.upload({'nanoTimeStamp': nanoTimeStamp})
# #vc = xx.run('nanoTimeStamp')
# print "running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000)
# print nanoTimeStamp
# #print vc

# print xx.run("nanotimestamp(402505204552000000l)")
# print xx.run("nanotime(402505204552000000l)")
#
# print "---------------------------------------------------"
# print "Test NanoTimestamp vector read/upload/read"
# timeStart = datetime.now()
# timeStamps = xx.run('2012.10.01T15:00:04.856123123 + rand(10000,10)')
# xx.upload({'timeStamps': np.array(timeStamps)})
# vc = xx.run('timeStamps')
# print "running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000)
# print timeStamps
# print vc
#
#
# print "Test nanotime vector read/upload/read"
# timeStart = datetime.now()
# times = xx.run('12:32:56.356000000 + (rand(1000000,10))')
# xx.upload({'times': np.array(times)})
# vc = xx.run('times')
# print "running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000)
# print times
# print vc
#
#
# print xx.run("table(12:32:56.356000000 + (rand(1000000,10)) as ns)")
# print xx.run("dict(1..3, 12:32:56.356000000 + rand(1000000,3))")
#
# print "Test NanoTimestamp scalar read/upload/read"
# timeStart = datetime.now()
# nanoTimeStamp = xx.run('2012.10.01T15:00:04.008567123')
# xx.upload({'nanoTimeStamp': nanoTimeStamp})
# vc = xx.run('nanoTimeStamp')
# print "running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000)
# print nanoTimeStamp
# print vc