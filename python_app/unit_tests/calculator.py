class Calculator:
    def __init__(self, a, b):
        self.a = a
        self.b = b

    def get_sum(self):
        return self.a + self.b
    
    def get_dif(self):
        return self.a - self.b
    
    def get_quotient(self):
        return self.a / self.b  
      
    def get_mult(self):
        return self.a * self.b
    
    def get_square(self):
        return self.a * self.a

myCalc = Calculator(a =2 ,b =5)

answer = myCalc.get_sum()




print(answer )

if __name__ == '__main__':
    Calculator