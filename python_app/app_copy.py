
import pandas as pd
from sqlalchemy import create_engine

 #function to load and clean data
def load_and_clean_csv(path, primary_key):
    """
    Loads a CSV file, cleans column names, and checks for duplicate primary keys and removed null primary keys.

    Parameters:
        path (str): Path to the CSV file.
        primary_key (str): Column name to check for duplicates.

    Returns:
        pd.DataFrame: Cleaned DataFrame.
    """
    # Load the CSV
    df = pd.read_csv(path, header=0, skiprows=0)

    # Clean column names: strip, lowercase, replace spaces with underscores
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')

    df = df[df[primary_key].notna()]

    # Check for duplicates in the primary key
    if df[primary_key].duplicated().any():
        print(f"Warning: Duplicate values found in primary key column '{primary_key}'")
        print(df[df[primary_key].duplicated(keep=False)])

    return df

# Convert 'maximum_days_to_borrow' to number of days
def parse_borrow_period(period):
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

def save_to_sql(df, table_name, engine, if_exists='replace'):
        """
        Saves a DataFrame to a SQL table using SQLAlchemy.

        Parameters:
            df (pd.DataFrame): The DataFrame to save.
            table_name (str): Name of the SQL table.
            engine (sqlalchemy.Engine): SQLAlchemy engine object.
            if_exists (str): What to do if the table exists ('fail', 'replace', 'append').
        """
        try:
            df.to_sql(table_name, con=engine, if_exists=if_exists, index=False)
            print(f"Saved '{table_name}' to database.")
        except Exception as e:
            print(f"Failed to save '{table_name}': {e}")


def main():
    books = load_and_clean_csv('../Data/03_Library Systembook.csv', primary_key='id')
    customers = load_and_clean_csv('../Data/03_Library SystemCustomers.csv', primary_key='customer_id')


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
                book_returned_date=lambda d: pd.to_datetime(d['book_returned'], format='%d/%m/%Y', errors='coerce'),
                maximum_days_to_borrow=lambda d: d['days_allowed_to_borrow'], 
                book_customer_fk=lambda d: d['customer_id'].astype('Int64'),
                
            )
            [['book_pk', 'book_name', 'book_checkout_date','book_returned_date','maximum_days_to_borrow','book_customer_fk']]
    )


    # Apply the parsing function
    books_ideal['max_borrow_days'] = books_ideal['maximum_days_to_borrow'].apply(parse_borrow_period)

    # Calculate the actual borrowing duration
    books_ideal['borrow_duration'] = (books_ideal['book_returned_date'] - books_ideal['book_checkout_date']).dt.days

    # Check if the book was returned late
    books_ideal['returned_overdue'] = books_ideal['borrow_duration'] > books_ideal['max_borrow_days']

    # Check if the Return Date is before the Checkout Date
    books_ideal['returned_before_checkout'] = books_ideal['borrow_duration'] < 0




    customers_ideal = (
        customers
            .assign(
                customer_pk=lambda d: d['customer_id'].astype('Int64'),
                        )
            [['customer_pk','customer_name']]
    )

    print(customers_ideal.head())

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

    # Create a SQLite database in the current directory
    engine = create_engine('sqlite:///library.db')

    save_to_sql(books_with_customers, 'books_with_customers', engine)
    save_to_sql(books_per_customer, 'books_per_customer', engine)

    df_filtered = pd.read_sql('''
    SELECT * FROM books_per_customer
    WHERE total_books_borrowed > 2
    ''', con=engine)

    print(df_filtered.head())


if __name__ == '__main__':
    main()


# Class Template
class dataCleaner():
    def __init__(self, filepath):
        loadCSV(filepath)
        self.books_ideal = []

    def myFunctions(self):
        pass

    def main(self):
        self.variableMyFunction * 2
    

if __name__ == '__main__':
    data_loader = dataCleaner('filepath')
    data_loader.startClean()