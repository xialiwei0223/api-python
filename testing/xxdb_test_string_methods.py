import sys
sys.path.append('..')
import unittest
from dolphindb import *
from xxdb_server import HOST, PORT

#setup db connection
xx = session()
xx.connect(HOST, PORT)


class TestStringMethods(unittest.TestCase):

    def test_upper(self):
        self.assertEqual(xx.run('"foo".upper()'), 'FOO')

    def test_lower(self):
        self.assertEqual(xx.run('"FOo".lower()'), 'foo')

    def test_concat(self):
        self.assertEqual(xx.run('"ab" concat "c"'), 'abc')

    def test_where(self):
        self.assertTrue(xx.run('"GOOG" in `FB`GOOG`'))
        self.assertFalse(xx.run('"AAPL" in `FB`GOOG`'))
        self.assertEqual(xx.run('["FB", "GOOG", ] find "GOOG"'), 1)


if __name__ == '__main__':
    unittest.main()