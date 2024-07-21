import yfinance as yf
import psycopg2
from datetime import datetime

# List of stock tickers
tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'TSLA', 'NFLX', 'UNH', 'JPM','NVDA']

# Define the periods for fetching stock data
end_date_2022 = '2022-12-31'
start_date_2023 = '2023-01-01'
today_date = datetime.now().strftime('%Y-%m-%d')

# Define database connection parameters
db_params = {
    'dbname': 'stock_data',
    'user': 'postgres',
    'password': 'nishat',
    'host': 'localhost',
    'port': '5432'
}

try:
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()




    # Create tables for storing stock data
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stock_data_2018_to_2022 (
            id SERIAL PRIMARY KEY,
            ticker VARCHAR(10),
            date DATE,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            volume BIGINT
        )
    ''')
    print("Table stock_data_2018_to_2022 created successfully.")

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stock_data_2023_onwards (
            id SERIAL PRIMARY KEY,
            ticker VARCHAR(10),
            date DATE,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            volume BIGINT,
            FOREIGN KEY (id) REFERENCES stock_data_2018_to_2022(id)
        )
    ''')
    print("Table stock_data_2023_onwards created successfully.")

    # Commit table creation to ensure the tables are available
    conn.commit()

    def process_and_insert_data(stock_data, ticker, table_name):
        # Reset the index to make 'Date' a column
        stock_data = stock_data.reset_index()

        # Convert the index to datetime.date type
        stock_data['Date'] = stock_data['Date'].dt.date
        stock_data = stock_data.drop(columns=['Dividends'])
        stock_data = stock_data.drop(columns=['Stock Splits'])
        # stock_data=stock_data.drop(columns=['Adj_Close'])

        # Add ticker column
        stock_data['Ticker'] = ticker

        # Debug: Print the first few rows of the DataFrame and column names
        print(f"Processing data for {ticker} into table {table_name}")
        # print(stock_data.head())
        print(stock_data.columns)

        # Insert data into the database row by row
        for index, row in stock_data.iterrows():
            try:
                # Check if 'Date' is already a date object
                if isinstance(row['Date'], str):
                    # Convert 'Date' to a date object
                    row_date = datetime.strptime(row['Date'], '%Y-%m-%d').date()
                else:
                    row_date = row['Date']

                # Check for duplicate entries
                cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE ticker = %s AND date = %s", (row['Ticker'], row_date))
                count = cursor.fetchone()[0]

                if count == 0:
                    cursor.execute(f'''
                        INSERT INTO {table_name} (ticker, date, open, high, low, close, volume)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ''', (row['Ticker'], row_date, row['Open'], row['High'], row['Low'], row['Close'], row['Volume']))
            except Exception as e:
                print(f"Error processing row {index} for ticker {ticker}: {e}")


    # Fetch and insert data from 2019 to 2023
    for ticker in tickers:
        try:
            print(f"Fetching data for {ticker} from 2018 to 2022...")
            stock = yf.Ticker(ticker)
            stock_data_2018_2022 = stock.history(start='2018-01-01', end=end_date_2022)
            process_and_insert_data(stock_data_2018_2022, ticker, 'stock_data_2018_to_2022')
            print(f"Data for {ticker} from 2018 to 2022 inserted successfully.")
        except Exception as e:
            print(f"Error fetching or inserting data for {ticker} from 2018 to 2022: {e}")

    # Fetch and insert data from 2024 onwards
    for ticker in tickers:
        try:
            print(f"Fetching data for {ticker} from 2023 onwards...")
            stock = yf.Ticker(ticker)
            stock_data_2023_onwards = stock.history(start=start_date_2023, end=today_date)
            process_and_insert_data(stock_data_2023_onwards, ticker, 'stock_data_2023_onwards')
            print(f"Data for {ticker} from 2023 onwards inserted successfully.")
        except Exception as e:
            print(f"Error fetching or inserting data for {ticker} from 2023 onwards: {e}")

    # Commit the transaction
    conn.commit()

    # Close the cursor and connection
    cursor.close()
    conn.close()

    print("Stock data has been successfully fetched and stored in PostgreSQL database.")

except Exception as e:
    print(f"Error: {e}")
