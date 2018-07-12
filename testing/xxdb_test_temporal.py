import unittest
import decimal
import numpy as np
import pandas as pd
import sys
sys.path.append('..')
from dolphindb import *
from xxdb_server import HOST, PORT

#setup db connection
xx = session()
xx.connect(HOST, PORT)

class TestTemporal(unittest.TestCase):
    def test_get_date(self):
        vc =xx.run('2018.03.14')
        self.assertEqual(vc.to_date(), date(2018, 03, 14))

    def test_upload_date(self):
        vdt = date(2018, 03, 14)
        xx.upload({"up_variable": Date.from_date(vdt)})
        vc = xx.run("up_variable")
        self.assertEqual(vc.to_date(), date(2018, 03, 14))

    def test_get_vector_date(self):
        vc = xx.run("2018.03.01 2017.04.02 2016.05.03")
        self.assertEqual(vc[1].to_date(), date(2017, 04, 02))

    def test_get_vector_date_withnull(self):
        vc = xx.run("[2018.03.01,,2016.05.03]")
        self.assertTrue(Date.isnull(vc[1]))

    def test_get_table_date(self):
        vc = xx.run("table(2018.03.01 2017.04.02 2016.05.03 as dt)")
        self.assertEqual(vc.iat[1, 0].to_date(), date(2017, 04, 02))

    def test_get_table_date_time(self):
        vc = xx.run("table(2018.03.01 2017.04.02 2016.05.03 as dt,[11:42:01,10:34:02,] as tm)")
        self.assertEqual(vc.iat[1, 0].to_date(), date(2017, 04, 02))
        self.assertEqual(vc.iat[1, 1].to_time(), time(10, 34, 02))
        self.assertTrue(Time.isnull(vc.iat[2, 1]))

    def test_get_leap(self):
        vc = xx.run('1904.02.29')
        self.assertEqual(vc.to_date(), date(1904, 02, 29))

        vc = xx.run('1904.01.01 + 365')
        self.assertEqual(vc.to_date(), date(1904, 12, 31))

        vc = xx.run('2100.03.01')
        self.assertEqual(vc.to_date(), date(2100, 3, 1))

    def test_get_datetime(self):
        vc = xx.run('2018.03.14T11:28:04')
        self.assertEqual(vc.to_datetime(), datetime(2018, 03, 14, 11, 28, 4))

    def test_upload_datetime(self):
        vdt = datetime(2018, 03, 14, 11, 28, 4)
        xx.upload({"up_variable": Datetime.from_datetime(vdt)})
        vc = xx.run("up_variable")
        self.assertEqual(vc.to_datetime(), datetime(2018, 03, 14, 11, 28, 4))

    def test_get_month(self):
        vc = xx.run('2018.03M')
        self.assertEqual(vc.to_date(), date(2018, 03, 1))

    def test_upload_month(self):
        vdt = date(2018, 03, 1)
        xx.upload({"up_variable": Month.from_date(vdt)})
        vc = xx.run("up_variable")
        self.assertEqual(vc.to_date(), date(2018, 03, 1))

    def test_get_minute(self):
        vc = xx.run('14:48m')
        self.assertEqual(vc.to_time(), time(14, 48))

    def test_upload_minute(self):
        vdt = time(14, 48)
        xx.upload({"up_variable": Minute.from_time(vdt)})
        vc = xx.run("up_variable")
        self.assertEqual(vc.to_time(), time(14, 48))

    def test_get_second(self):
        vc = xx.run('15:41:45')
        self.assertEqual(vc.to_time(), time(15, 41, 45))

    def test_upload_second(self):
        vdt = time(15, 41, 45)
        xx.upload({"up_variable": Second.from_time(vdt)})
        vc = xx.run("up_variable")
        self.assertEqual(vc.to_time(), time(15, 41, 45))

    def test_get_time(self):
        vc = xx.run('15:41:45.123')
        self.assertEqual(vc.to_time(), time(15, 41, 45, 123000))
    pass

    def test_upload_time(self):
        vdt = time(15, 41, 45, 123000)
        xx.upload({"up_variable": Time.from_time(vdt)})
        vc = xx.run("up_variable")
        self.assertEqual(vc.to_time(), time(15, 41, 45, 123000))

    def test_get_timestamp(self):
        vc = xx.run('2018.03.14T15:41:45.123')
        self.assertEqual(vc.to_datetime(), datetime(2018, 3, 14, 15, 41, 45, 123000))

    def test_upload_timestamp(self):
        vdt = datetime(2018, 3, 14, 15, 41, 45, 123000)
        xx.upload({"up_variable": Timestamp.from_datetime(vdt)})
        vc = xx.run("up_variable")
        self.assertEqual(vc.to_datetime(), datetime(2018, 3, 14, 15, 41, 45, 123000))

    def test_get_nanotime(self):
        vc = xx.run('15:41:45.123456789')
        #the precision of python time is microsecond =  nanosecond % 1000000
        self.assertEqual(vc.to_nanotime(), time(15, 41, 45, 123456))

    def test_upload_nanotime(self):
        vdt = time(15, 41, 45, 456789)
        xx.upload({"up_variable": NanoTime.from_time(vdt)})
        vc = xx.run("up_variable")
        self.assertEqual(vc.to_nanotime(), time(15, 41, 45, 456789))

    def test_get_nanotimestamp(self):
        vc = xx.run('2018.03.14T15:41:45.123222321')
        self.assertEqual(vc.to_nanotimestamp(), datetime(2018, 03, 14, 15, 41, 45, 123222))

    def test_upload_nanotimestamp(self):
        vdt = datetime(2018, 03, 14, 15, 41, 45, 123222)
        xx.upload({"up_variable": NanoTimestamp.from_datetime(vdt)})
        vc = xx.run("up_variable")
        self.assertEqual(vc.to_nanotimestamp(), datetime(2018, 03, 14, 15, 41, 45, 123222))


if __name__ == '__main__':
    unittest.main()