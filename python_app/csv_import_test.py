
import pandas as pd
import unittest
import unittest.mock import patch
from class_based_app_w_mode import dataCleaner 

class TestDataCleaner(unittest.TestCase):

    def setup(self):
        self.cleaner = dataCleaner()

    @patch('pandas.read_csv')
    def test_column_cleaning_and_null_values(self, mock_read_csv):
        test_data = pd.DataFrame({
            ' ID ' : [1,2 , None],
            'Book TITLE' : [ 'Book A', 'GooseBumps', 'Nomad'],
            'CUSTOMER ID' : [1,2,3]
        })
    mock_read_csv.return_value = test_data 

    cleaned_df = self.cleaner.load_and_clean_csv"dummy()   


    def clean_column(self):    
        self.assertEqual(self.operator.load_and_clean_csv(), "table_id", 'The column name is wrong')

    def tearDown(self):
        print('All tests run')            


if __name__ == '__main__':
    unittest.main()

    