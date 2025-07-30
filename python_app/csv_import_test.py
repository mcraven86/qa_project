import unittest
from unittest.mock import patch
import pandas as pd
from class_based_app_w_mode import dataCleaner

class TestDataCleaner(unittest.TestCase):

    def setUp(self):
        self.cleaner = dataCleaner()

    @patch('pandas.read_csv')
    def test_column_cleaning_and_null_removal(self, mock_read_csv):
        # Create a mock DataFrame with messy column names and a null primary key
        test_data = pd.DataFrame({
            ' ID ': [1, 2, None],
            ' Book Title ': ['Book A', 'Book B', 'Book C'],
            ' Customer ID ': [101, 102, 103]
        })

        # Mock pd.read_csv to return this DataFrame
        mock_read_csv.return_value = test_data

        # Call the method
        cleaned_df = self.cleaner.load_and_clean_csv("dummy_path.csv", primary_key='id')

        # Check that column names are cleaned
        expected_columns = ['id', 'book_title', 'customer_id']
        self.assertListEqual(list(cleaned_df.columns), expected_columns)

        # Check that the row with null 'id' is removed
        self.assertEqual(len(cleaned_df), 2)
        self.assertFalse(cleaned_df['id'].isnull().any())

    def tearDown(self):
        print("All tests completed.")

if __name__ == '__main__':
    unittest.main()
