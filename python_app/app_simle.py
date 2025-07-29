import pandas as pd
from sqlalchemy import create_engine

class DataCleaner:
    def __init__(self):
        self.engine = create_engine('sqlite:///library_temp.db')

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
            return int(period.split()[0]) * 7
        elif 'day' in period:
            return int(period.split()[0])
        return None

    def save_to_sql(self, df, table_name, if_exists='replace'):
        try:
            df.to_sql(table_name, con=self.engine, if_exists=if_exists, index=False)
            print(f" Saved '{table_name}' to database.")
        except Exception as e:
            print(f" Failed to save '{table_name}': {e}")

    def main(self):
        books = self.load_and_clean_csv('../Data/03_Library Systembook.csv', primary_key='id')

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
                        d['book_returned'], format='%d/%m/%Y', errors='coerce'
                    ),
                    maximum_days_to_borrow=lambda d: d['days_allowed_to_borrow'], 
                    book_customer_fk=lambda d: d['customer_id'].astype('Int64'),
                )
                [['book_pk', 'book_name', 'book_checkout_date', 'book_returned_date', 'maximum_days_to_borrow', 'book_customer_fk']]
        )

        self.save_to_sql(books_ideal, 'books_ideal')

# Entry point
if __name__ == '__main__':
    cleaner = DataCleaner()
    cleaner.main()
