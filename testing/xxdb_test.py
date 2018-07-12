import numpy as np
import copy
import sys
sys.path.append('..')
from dolphindb import *

from xxdb_server import HOST, PORT

if __name__ == '__main__':
    xx = session()
    success = xx.connect(HOST, PORT)
    if success:

        print "---------------------------------------------------"
        print "Testing double Vector"
        timeStart = datetime.now()
        vc = xx.run('rand(1000.0,10000)')
        print "running time (in millisecond): " + str((datetime.now() - timeStart).microseconds/1000)
        print len(vc), vc

        print "---------------------------------------------------"
        print "Testing String Vector"
        timeStart = datetime.now()
        vc = xx.run('rand(`IBM`MSFT`GOOG`BIDU,10000)')
        print "running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000)
        print vc

        print "---------------------------------------------------"
        print "Testing Dictionary"
        timeStart = datetime.now()
        vc = xx.run('dict(1 2 3, `IBM`MSFT`GOOG)')
        print "running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000)
        print vc

        print "---------------------------------------------------"
        print "Testing matrix"
        timeStart = datetime.now()
        matrix, rowlabels, colLables = xx.run('1..6$3:2')
        print "running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000)
        print matrix

        print "---------------------------------------------------"
        print "Testing table"
        timeStart = datetime.now()
        table_str = "n=20000\n"
        table_str += "syms=`IBM`C`MS`MSFT`JPM`ORCL`BIDU`SOHU`GE`EBAY`GOOG`FORD`GS`PEP`USO`GLD`GDX`EEM`FXI`SLV`SINA`BAC`AAPL`PALL`YHOO`KOH`TSLA`CS`CISO`SUN\n"
        table_str += "t1=table(2016.08.09 09:30:00.000+rand(18000,n) as timestamp,rand(syms,n) as sym,100*(1+rand(100,n)) as qty,5.0+rand(100.0,n) as price);\n"
        table_str += "select sym,qty,price from t1 where price>9"
        df = xx.run(table_str)
        print "running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000)
        print df

        print "---------------------------------------------------"
        print "Testing function add integer"
        timeStart = datetime.now()
        vc = xx.run('add',1334,-334)
        print "running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000)
        print vc

        print "---------------------------------------------------"
        print "Testing function sub float"
        timeStart = datetime.now()
        vc = xx.run('sub', 97.62, -32.38)
        print "running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000)
        print vc

        print "---------------------------------------------------"
        print "Testing function add string"
        timeStart = datetime.now()
        vc = xx.run('add', 'add', 'string')
        print "running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000)
        print vc

        print "---------------------------------------------------"
        print "Testing function sum  list float"
        timeStart = datetime.now()
        vc = xx.run('sum', [1.0, 2.0, 3.0])
        print "running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000)
        print vc

        print "---------------------------------------------------"
        print "Testing function dict keys"
        timeStart = datetime.now()
        vc = xx.run('keys', {"ibm":100.0, "ms":120.0, "c": 130.0})
        print "running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000)
        print vc

        print "---------------------------------------------------"
        print "Testing function dict values"
        timeStart = datetime.now()
        vc = xx.run('values', {"ibm":100.0, "ms":120.0, "c": 130.0})
        print "running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000)
        print vc

        print "---------------------------------------------------"
        print "Testing function sum  numpy array int32 "
        timeStart = datetime.now()
        vc = xx.run("sum", np.array([100000, 200000, 300000]))
        print "running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000)
        print vc

        print "---------------------------------------------------"
        print "Testing function sum  numpy array int64 "
        timeStart = datetime.now()
        vc = xx.run("sum", np.int64([1e15, 2e15, 3e15]))
        print "running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000)
        print vc

        print "---------------------------------------------------"
        print "Testing function sum  numpy array float64 "
        timeStart = datetime.now()
        vc = xx.run("sum", np.array([100000.0, 200000.0, 300000.0]))
        print "running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000)
        print vc

        print "---------------------------------------------------"
        print "Testing function sum  numpy array bool "
        timeStart = datetime.now()
        vc = xx.run("sum", np.bool_([True, False, True, False]))
        print "running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000)
        print vc

        print "---------------------------------------------------"
        print "Testing function reverse str vector"
        timeStart = datetime.now()
        vc = xx.run("reverse", np.array(["1", "2", "3"],dtype="str"))
        print "running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000)
        print vc

        print "---------------------------------------------------"
        print "Testing function user defined function"
        timeStart = datetime.now()
        xx.run("def f(a,b) {return a+b};")
        vc = xx.run("f", 1, 2.0)
        print "running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000)
        print vc

        print "---------------------------------------------------"
        print "Testing function flatten int matrix"
        timeStart = datetime.now()
        vc = xx.run("flatten", np.int32([[1, 2, 3], [4, 5, 6]]))
        print "running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000)
        print vc

        print "---------------------------------------------------"
        print "Testing function flatten double matrix"
        timeStart = datetime.now()
        vc = xx.run("flatten", np.double([[1, 2, 3], [4.0, 5.0, 6.0]]))
        print "running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000)
        print vc

        print "---------------------------------------------------"
        print "Testing matrix upload"
        timeStart = datetime.now()
        (a, _, _) = xx.run("cross(+, 1..5, 1..5)")
        (b, _, _) = xx.run("1..25$5:5")
        nameObjectDict = {'a':a, 'b':b}
        xx.upload(nameObjectDict)
        (vc, _, _) =xx.run("a+b")
        print "running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000)
        print vc


        print "---------------------------------------------------"
        print "Test set read"
        timeStart = datetime.now()
        vc = xx.run('set([5, 5, 3, 4])')
        print vc

        print "---------------------------------------------------"
        print "Test set upload"
        timeStart = datetime.now()
        x = {5, 5, 4, 3}
        y = {8, 9, 9, 4, 6}
        nameObjectDict = {'x': x, 'y': y}
        xx.upload(nameObjectDict)
        vc = xx.run('x | y')
        print "running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000)
        print vc

        print "---------------------------------------------------"
        print "Test pair read"
        timeStart = datetime.now()
        vc = xx.run('3:4')
        print "running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000)
        print vc

        print "---------------------------------------------------"
        print "Testing function cast double matrix"
        timeStart = datetime.now()
        x = np.double([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]])
        vc = xx.run("cast", np.double([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]]), Pair(2,3))
        print "running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000)
        print vc[0]

        print "---------------------------------------------------"
        print "Test any vector"
        timeStart = datetime.now()
        x = [1, 2, "a", 'b']
        xx.upload({'x': x})
        vc = xx.run('x[1:3]')
        print "running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000)
        print vc

        print "---------------------------------------------------"
        print "Testing Date scalar"
        timeStart = datetime.now()
        vc = xx.run('2012.10.01')
        print "running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000)
        print vc

        print "Testing Date scalar"
        timeStart = datetime.now()
        vc = xx.run('1904.02.29')
        print "running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000)
        print vc

        print "Testing Date scalar"
        timeStart = datetime.now()
        vc = xx.run('1904.01.01 + 365')
        print "running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000)
        print vc

        print "---------------------------------------------------"
        print "Test date vector read/upload/read"
        timeStart = datetime.now()
        dates = xx.run('2012.10.01 + rand(1000,1000)')
        xx.upload({'dates': np.array(dates)})
        vc = xx.run('dates')
        print "running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000)
        print dates
        print vc

        print "---------------------------------------------------"
        print "Test month vector read/upload/read"
        timeStart = datetime.now()
        months = xx.run('2012.01M+rand(11,10)')
        xx.upload({'months': months})
        vc = xx.run('months')
        print "running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000)
        print months
        print vc

        print "---------------------------------------------------"
        print "Test time vector read/upload/read"
        timeStart = datetime.now()
        times = xx.run('12:32:56.356 + (rand(1000000,10))')
        xx.upload({'times': np.array(times)})
        vc = xx.run('times')
        print "running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000)
        print times
        print vc

        print "---------------------------------------------------"
        print "Test nanotime vector read/upload/read"
        timeStart = datetime.now()
        times = xx.run('12:32:56.356000000 + (rand(1000000,10))')
        xx.upload({'times': np.array(times)})
        vc = xx.run('times')
        print "running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000)
        print times
        print vc

        print "---------------------------------------------------"
        print "Test minute vector read/upload/read"
        timeStart = datetime.now()
        minutes = xx.run('12:30m+rand(100,10)')
        xx.upload({'minutes': np.array(minutes)})
        vc = xx.run('minutes')
        print "running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000)
        print minutes
        print vc

        print "---------------------------------------------------"
        print "Test second vector read/upload/read"
        timeStart = datetime.now()
        seconds = xx.run('12:56:38+rand(1000,10)')
        xx.upload({'seconds': np.array(seconds)})
        vc = xx.run('seconds')
        print "running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000)
        print seconds
        print vc

        print "---------------------------------------------------"
        print "Test datetime vector read/upload/read"
        timeStart = datetime.now()
        datetimes = xx.run('2012.10.01T15:00:04 + rand(10000,10)')
        xx.upload({'datetimes': np.array(datetimes)})
        vc = xx.run('datetimes')
        print "running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000)
        print datetimes
        print vc

        print "---------------------------------------------------"
        print "Test Timestamp scalar read/upload/read"
        timeStart = datetime.now()
        timeStamp = xx.run('2012.10.01T15:00:04.008')
        xx.upload({'timeStamp':timeStamp})
        vc = xx.run('timeStamp')
        print "running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000)
        print timeStamp
        print vc

        print "---------------------------------------------------"
        print "Test timeStamp vector read/upload/read"
        timeStart = datetime.now()
        timeStamps = xx.run('2012.10.01T15:00:04.008 + rand(10000,10)')
        xx.upload({'timeStamps': np.array(timeStamps)})
        vc = xx.run('timeStamps')
        print "running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000)
        print timeStamps
        print vc

        print "Test NanoTimestamp scalar read/upload/read"
        timeStart = datetime.now()
        nanoTimeStamp = xx.run('2012.10.01T15:00:04.008567123')
        xx.upload({'nanoTimeStamp': nanoTimeStamp})
        vc = xx.run('nanoTimeStamp')
        print "running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000)
        print nanoTimeStamp
        print vc

        print "---------------------------------------------------"
        print "Test NanoTimestamp vector read/upload/read"
        timeStart = datetime.now()
        timeStamps = xx.run('2012.10.01T15:00:04.856123123 + rand(10000,10)')
        xx.upload({'timeStamps': np.array(timeStamps)})
        vc = xx.run('timeStamps')
        print "running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000)
        print timeStamps
        print vc

        print "---------------------------------------------------"
        print "Testing table upload"
        timeStart = datetime.now()
        df = pd.DataFrame({'id': np.int32([1, 2, 3, 4, 3]),
                           'value':  np.double([7.8, 4.6, 5.1, 9.6, 0.1]),
                           'x': np.int32([5, 4, 3, 2, 1])
                           })
        df2 = pd.DataFrame({'id': np.int32([3, 1]),
                           'qty':  np.int32([500, 800]),
                           'x': np.double([66.0, 88.0])
                           })
        nameObjectDict = {'t1': df, 't2': df2}
        xx.upload(nameObjectDict)
        vc = xx.run("lj(t1, t2, `id)")
        print "running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000)
        print vc

        print "---------------------------------------------------"
        print "Test double nan"
        timeStart = datetime.now()
        l = [1.0, 2.0, np.nan, 3.0, 4.0 ,np.nan]
        l2 = map(lambda x:  doubleNan if pd.isnull(x)  else x, l)
        xx.upload({'l2':l2})
        vc = xx.run('l2')
        print "running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000)
        print l, len(l)
        print l2, len(l2)
        print vc, len(vc)

        print "---------------------------------------------------"
        print "Test int nan"
        timeStart = datetime.now()
        l = [1, 2, np.nan, 3, 4, np.nan]
        l2 = map(lambda x:  intNan if pd.isnull(x)  else x, l)
        xx.upload({'l2':l2})
        vc = xx.run('l2')
        print "running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000)
        print l, len(l)
        print l2, len(l2)
        print vc, len(vc)

        print "---------------------------------------------------"
        print "Test datetime nan"
        timeStart = datetime.now()
        datetimes = xx.run('2012.10.01T15:00:04 + rand(10000,10)')
        datetimes2 = copy.copy(datetimes)
        datetimes2[0] = Datetime.null()
        datetimes2[3] = Datetime.null()
        datetimes2[4] = Datetime.null()
        datetimes2[9] = Datetime.null()
        xx.upload({'datetimes2':datetimes2})
        vc = xx.run('datetimes2')
        l = map(lambda x: Datetime.isnull(x), vc)
        print "running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000)
        print datetimes, len(datetimes)
        print datetimes2, len(datetimes2)
        print vc, len(vc)
        print l

        print xx.run("nanotimestamp(long(nanotimestamp(now())))")