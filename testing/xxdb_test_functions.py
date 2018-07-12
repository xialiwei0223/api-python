import unittest
import decimal
import numpy as np
import sys
sys.path.append('..')
from dolphindb import *

from xxdb_server import HOST, PORT

#setup db connection
xx = session()
xx.connect(HOST, PORT)


def run(script):
    return xx.run(script=script)


def handleException(ex):
    pass

class TestFunctions(unittest.TestCase):

    def test_abs(self):
        self.assertEqual(run('abs -2.0'), 2.0)
        self.assertEqual(set(run('abs -2 -3 4')), set([2,3,4]))
        self.assertTrue(run("isNull(abs(00c))"))
        self.assertEqual(run("abs(10c)"), 10)

    def test_accumulate(self):
        self.assertTrue(run("(1..3 +:A 1..6$3:2) == 2 4 6 6 9 12$3:2"))
        self.assertTrue(np.array_equal(run("(1..3 +:A 1..6$3:2)")[0], np.array([[2,6], [4,9], [6,12]])))
        d = decimal.Decimal('1.1')
        self.assertTrue(np.array_equal(map(lambda x: round(x,1), run('accumulate(add, 7.8f 2.4f NULL -5.6f 11.5f)')),
                                       np.array([7.8,10.2,10.2,4.6,16.1])))
    def test_asin(self):
        self.assertTrue((run("asin:E -1.5f 12f 0.75f")[2] - 0.848062)<1e-6)

if __name__ == '__main__':
    unittest.main()