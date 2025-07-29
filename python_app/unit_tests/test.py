import pandas as pd
import unittest
from calculator import Calculator

class TestOperator(unittest.TestCase):

    def test_sum(self):
        calculation = Calculator(2,2)
        answer = calculation.get_sum()
        self.assertEqual(answer, 4 , 'The Sum is wrong')

    def test_dif(self):
        calculation = Calculator(2,2)
        answer = calculation.get_dif()
        self.assertEqual(answer, 0 , 'The Sum is wrong')    

    def test_quotiant(self):
        calculation = Calculator(2,2)
        answer = calculation.get_quotient()
        self.assertEqual(answer, 1 , 'The Sum is wrong')  

    def test_mult(self):
        calculation = Calculator(2,2)
        answer = calculation.get_mult()
        self.assertEqual(answer, 4 , 'The Sum is wrong')        

    def test_square(self):
        calculation = Calculator(2,1)
        answer = calculation.get_square()
        self.assertEqual(answer, 4 , 'The Sum is wrong')            


if __name__ == '__main__':
    unittest.main()