
import pandas as pd

# Load the CSV files
books = pd.read_csv('../Data/03_Library Systembook.csv', header=0, skiprows=0)
customers = pd.read_csv('../Data/03_Library SystemCustomers.csv', header=0, skiprows=0)


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


books_ideal = (
    books
        .loc[books['Id'].notna()]
        .assign(
            book_pk=lambda d: d['Id'].astype('Int64'),
            book_name=lambda d: d['Books'],
            book_checkout_date=lambda d: pd.to_datetime(
                d['Book checkout'].str.replace('"', '').str.strip(), 
                format='%d/%m/%Y', 
                errors='coerce'
            ),
            book_returned_date=lambda d: pd.to_datetime(d['Book Returned'], format='%d/%m/%Y', errors='coerce'),
            maximum_days_to_borrow=lambda d: d['Days allowed to borrow'], 
            book_customer_fk=lambda d: d['Customer ID'].astype('Int64'),
            
        )
        [['book_pk', 'book_name', 'book_checkout_date','book_returned_date','maximum_days_to_borrow','book_customer_fk']]
)


# Apply the parsing function
books_ideal['max_borrow_days'] = books_ideal['maximum_days_to_borrow'].apply(parse_borrow_period)

# Calculate the actual borrowing duration
books_ideal['borrow_duration'] = (books_ideal['book_returned_date'] - books_ideal['book_checkout_date']).dt.days

# Check if the book was returned late
books_ideal['returned_overdue'] = books_ideal['borrow_duration'] > books_ideal['max_borrow_days']


print(books_ideal.head())

customers_ideal = (
    customers
        .loc[customers['Customer ID'].notna()]
        .assign(
            Customer_pk=lambda d: d['Customer ID'].astype('Int64'),
            Customer_name=lambda d: d['Customer Name'],
                    )
        [['Customer_pk','Customer_name']]
)

# Display the first few rows
#print(customers_ideal.head())
