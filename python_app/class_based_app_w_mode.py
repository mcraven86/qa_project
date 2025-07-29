import pandas as pd
from sqlalchemy import create_engine
import argparse
import csv


class dataCleaner:
    def __init__(self, save_to_sql=False, save_to_csv=False):
        self.save_sql = save_to_sql
        self.save_csv = save_to_csv
        self.books_ideal = []

    def load_and_clean_csv(self, path, primary_key):
        df = pd.read_csv(path, header=0, skiprows=0)
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
        df = df[df[primary_key].notna()]

        if df[primary_key].duplicated().any():
            print(f"Warning: Duplicate values found in primary key column '{primary_key}'")
            print(df[df[primary_key].duplicated(keep=False)])

        return df

    def parse_borrow_period(self, period):
        if pd.isna(period):
            return None
        period = str(period).lower().strip()
        if 'week' in period:
            num = int(period.split()[0])
            return num * 7
        elif 'day' in period:
            num = int(period.split()[0])
            return num
        return None

    def save_data(self, df, filename="output_data", engine=None):
        if self.save_sql:
            self._save_to_sql(df, filename, engine)
        if self.save_csv:
            self._save_to_csv(df, filename)
        if not self.save_sql and not self.save_csv:
            print("No saving mode selected. Saving to both SQL and CSV.")
            self._save_to_sql(df, filename, engine)
            self._save_to_csv(df, filename)

    def _save_to_sql(self, df, table_name, engine, if_exists='replace'):
        try:
            df.to_sql(table_name, con=engine, if_exists=if_exists, index=False)
            print(f"Saved '{table_name}' to database.")
        except Exception as e:
            print(f"Failed to save '{table_name}': {e}")

    def _save_to_csv(self, df, filename):
        try:
            df.to_csv(f"{filename}.csv", index=False)
            print(f"Data saved to {filename}.csv (CSV)")
        except IOError as e:
            print(f"Error saving to CSV: {e}")

    def main(self, args):
        books = self.load_and_clean_csv(
            'C:/Users/Admin/Desktop/qa_project/Data/03_Library_Systembook.csv',
            primary_key='id'
        )
        customers = self.load_and_clean_csv(
            'C:/Users/Admin/Desktop/qa_project/Data/03_Library SystemCustomers.csv',
            primary_key='customer_id'
        )

        books_ideal = (
            books
            .assign(
                book_pk=lambda d: d['id'].astype('Int64'),
                book_name=lambda d: d['books'],
                book_checkout_date=lambda d: pd.to_datetime(
                    d['book_checkout'].str.replace('"', '').str.strip(),
                    format='%d/%m/%Y',
                    errors='coerce'
                ),
                book_returned_date=lambda d: pd.to_datetime(
                    d['book_returned'],
                    format='%d/%m/%Y',
                    errors='coerce'
                ),
                maximum_days_to_borrow=lambda d: d['days_allowed_to_borrow'],
                book_customer_fk=lambda d: d['customer_id'].astype('Int64'),
            )
            [['book_pk', 'book_name', 'book_checkout_date', 'book_returned_date',
              'maximum_days_to_borrow', 'book_customer_fk']]
        )

        books_ideal['max_borrow_days'] = books_ideal['maximum_days_to_borrow'].apply(self.parse_borrow_period)
        books_ideal['borrow_duration'] = (books_ideal['book_returned_date'] - books_ideal['book_checkout_date']).dt.days
        books_ideal['returned_overdue'] = books_ideal['borrow_duration'] > books_ideal['max_borrow_days']
        books_ideal['returned_before_checkout'] = books_ideal['borrow_duration'] < 0

        customers_ideal = (
            customers
                .assign(
                    customer_pk=lambda d: d['customer_id'].astype('Int64'),
                            )
                [['customer_pk','customer_name']]
        )

        #print(customers_ideal.head())

        books_with_customers = (
            books_ideal
                .merge(customers_ideal, left_on='book_customer_fk', right_on='customer_pk', how='left')
                .drop(columns=['book_customer_fk']) 
        )


        # Group by Customer_pk and count the number of books borrowed

        books_per_customer = (
            books_with_customers
                .groupby(['customer_pk', 'customer_name'])
                .agg(
                    total_books_borrowed=('book_pk', 'count'),
                    first_checkout_date=('book_checkout_date', 'min'),
                    last_return_date=('book_returned_date', 'max')
                )
                .reset_index()
        )

        engine = create_engine('sqlite:///library.db')
        self.save_data(books_per_customer, filename="books_per_customer", engine=engine)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Save data to SQL, CSV, or both.")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--sql", action="store_true", help="Save data to SQL.")
    group.add_argument("--csv", action="store_true", help="Save data to CSV.")
    args = parser.parse_args()

    cleaner = dataCleaner(save_to_sql=args.sql, save_to_csv=args.csv)
    cleaner.main(args)
