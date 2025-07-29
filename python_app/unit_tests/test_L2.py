import pandas as pd
import unittest
from calculator import Calculator

class TestCalculations(unittest.TestCase):

    def setup(self):
        self.operator = Calculator(a=8, b=2)

    def test_sum(self):    
        self.assertEqual(self.operator.get_sum(), 10, 'The Sum is wrong')

    def test_dif(self):
        self.assertEqual(self.operator.get_dif(), 6, 'The Sum is wrong') 

    def test_quotiant(self):
        self.assertEqual(self.operator.get_quotient(), 4, 'The Sum is wrong') 

    def tearDown(self):
        print('All tests run')            


if __name__ == '__main__':
    unittest.main()