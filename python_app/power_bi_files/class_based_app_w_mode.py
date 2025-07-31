import pandas as pd
from sqlalchemy import create_engine
import argparse
import csv


class dataCleaner:
    def __init__(self, save_to_sql=False, save_to_csv=False):
        self.save_sql = save_to_sql
        self.save_csv = save_to_csv
        self.books_ideal = []
        self.audit_log = pd.DataFrame(columns=['function_name', 'action', 'records_effected'])


    def load_and_clean_csv(self, path, primary_key):
        df = pd.read_csv(path, header=0, skiprows=0)
        csv_name = path.split('/')[-1]
                                   
        original_row_count = len(df)
        self.log_audit('load_and_clean_csv', f"{csv_name} loaded rows", original_row_count)

        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
        self.log_audit('load_and_clean_csv', f"{csv_name} columns renamed", len(df.columns))

        df = df[df[primary_key].notna()]
        filtered_row_count = len(df)
        self.log_audit('load_and_clean_csv', f"{csv_name} filtered rows", original_row_count - filtered_row_count)

        pk_violations = df[primary_key].duplicated().sum()
        if pk_violations > 0:
            print(f"Warning: Duplicate values found in primary key column '{primary_key}'")
            print(df[df[primary_key].duplicated(keep=False)])
        self.log_audit('load_and_clean_csv', f"{csv_name} rows with pk violation", pk_violations)

        return df

    
    def log_audit(self, function_name, action, records_effected):
        new_entry = pd.DataFrame([{
            'function_name': function_name,
            'action': action,
            'records_effected': records_effected
        }])
        self.audit_log = pd.concat([self.audit_log, new_entry], ignore_index=True)


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
            self.log_audit('_save_to_sql', f"{table_name} saved to SQL", len(df))
            print(f"Saved '{table_name}' to database.")
        except Exception as e:
            print(f"Failed to save '{table_name}': {e}")


    def _save_to_csv(self, df, filename):
        output_path = "C:/Users/Admin/Desktop/qa_project/Data/output.csv"
        try:
            df.to_csv(output_path, index=False)
            self.log_audit('_save_to_csv', f"{filename} saved to CSV", len(df))
            print(f"Data saved to {filename}.csv (CSV)")
        except IOError as e:
            print(f"Error saving to CSV: {e}")



    def remove_quotes_from_field(self, series):
        quote_count = series.str.count('"').sum()
        cleaned_series = series.str.replace('"', '', regex=False)
        self.log_audit('remove_quotes_from_field', f"column {series.name} bad data cleaned", quote_count)
        return cleaned_series
    
    
    def flag_returned_before_checkout(self, df, checkout_col, return_col, new_col_name):
        condition = (df[return_col] - df[checkout_col]).dt.days < 0
        df[new_col_name] = condition

        count_flagged = condition.sum()
        self.log_audit(
            'flag_returned_before_checkout',
            f"{new_col_name} flagged as returned before checkout",
            count_flagged
        )
        return df

     

    def main(self, args):
        books = self.load_and_clean_csv(
            'C:/Users/Admin/Desktop/qa_project/Data/03_Library_Systembook.csv',#C:/Users/Admin/Desktop/qa_project/Data/
            primary_key='id'
        )
        customers = self.load_and_clean_csv(
            'C:/Users/Admin/Desktop/qa_project/Data/03_Library SystemCustomers.csv',#C:/Users/Admin/Desktop/qa_project/Data/
            primary_key='customer_id'
        )

        books_ideal = (
            books
            .assign(
                book_pk=lambda d: d['id'].astype('Int64'),
                book_name=lambda d: d['books'],
                book_checkout_date=lambda d: pd.to_datetime(
                    self.remove_quotes_from_field(d['book_checkout']).str.strip(),
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
                
        books_ideal = self.flag_returned_before_checkout(
            books_ideal,
            checkout_col='book_checkout_date',
            return_col='book_returned_date',
            new_col_name='returned_before_checkout'
        )


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
        #self.save_data(books_per_customer, filename="books_per_customer", engine=engine)
        self.save_data(books_with_customers,filename="books_with_customer", engine=engine)
        
        print("\nAudit Log:")
        print(self.audit_log)

        # Optional: Save to CSV
        self.audit_log.to_csv("C:/Users/Admin/Desktop/qa_project/Data/audit_log.csv", index=False)



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Save data to SQL, CSV, or both.")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--sql", action="store_true", help="Save data to SQL.")
    group.add_argument("--csv", action="store_true", help="Save data to CSV.")
    args = parser.parse_args()

    cleaner = dataCleaner(save_to_sql=args.sql, save_to_csv=args.csv)
    cleaner.main(args)

    


