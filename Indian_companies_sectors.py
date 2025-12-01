import yfinance as yf
import mysql.connector
from mysql.connector import Error
import pandas as pd
import time
from typing import Optional, Dict
import os
from dotenv import load_dotenv

load_dotenv()

class CompanyDataExtractor:
    def __init__(self, host: str, database: str, user: str, password: str):
        """Initialize database connection parameters"""
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.connection = None
    
    def connect_db(self):
        """Establish database connection"""
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password
            )
            if self.connection.is_connected():
                print("Successfully connected to database")
                return True
        except Error as e:
            print(f"Error connecting to database: {e}")
            return False
    
    def get_company_symbols(self) -> list:
        """Fetch company symbols from india_listed_companies table"""
        if not self.connection or not self.connection.is_connected():
            print("Not connected to database")
            return []
        
        try:
            cursor = self.connection.cursor()
            query = "SELECT Symbol, Name FROM india_listed_companies"
            cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()
            print(f"Fetched {len(results)} company symbols")
            return results
        except Error as e:
            print(f"Error fetching symbols: {e}")
            return []
    
    def fetch_company_data(self, symbol: str) -> Optional[Dict]:
        """Fetch company data from yfinance"""
        try:
            # For Indian stocks, append .NS (NSE) or .BO (BSE) suffix
            ticker_symbol = symbol
            ticker = yf.Ticker(ticker_symbol)
            info = ticker.info
            
            # Extract required fields
            data = {
                'symbol': symbol,
                'revenue': info.get('totalRevenue', None),
                'market_cap': info.get('marketCap', None),
                'industry': info.get('industry', None),
                'sector': info.get('sector', None)
            }
            
            return data
        except Exception as e:
            print(f"Error fetching data for {symbol}: {e}")
            return None
    
    def create_table(self):
        """Create India_listed_companies_information table"""
        if not self.connection or not self.connection.is_connected():
            print("Not connected to database")
            return False
        
        try:
            cursor = self.connection.cursor()
            
            # Drop table if exists (optional - remove if you want to keep existing data)
            # cursor.execute("DROP TABLE IF EXISTS India_listed_companies_information")
            
            create_table_query = """
            CREATE TABLE IF NOT EXISTS India_listed_companies_information (
                id INT AUTO_INCREMENT PRIMARY KEY,
                symbol VARCHAR(512) NOT NULL UNIQUE,
                revenue BIGINT,
                market_cap BIGINT,
                industry VARCHAR(512),
                sector VARCHAR(512),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            )
            """
            cursor.execute(create_table_query)
            self.connection.commit()
            cursor.close()
            print("Table 'India_listed_companies_information' created successfully")
            return True
        except Error as e:
            print(f"Error creating table: {e}")
            return False
    
    def insert_company_data(self, data: Dict) -> bool:
        """Insert company data into database"""
        if not self.connection or not self.connection.is_connected():
            print("Not connected to database")
            return False
        
        try:
            cursor = self.connection.cursor()
            
            insert_query = """
            INSERT INTO India_listed_companies_information 
            (symbol, revenue, market_cap, industry, sector)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            revenue = VALUES(revenue),
            market_cap = VALUES(market_cap),
            industry = VALUES(industry),
            sector = VALUES(sector)
            """
            
            values = (
                data['symbol'],
                data['revenue'],
                data['market_cap'],
                data['industry'],
                data['sector']
            )
            
            cursor.execute(insert_query, values)
            self.connection.commit()
            cursor.close()
            return True
        except Error as e:
            print(f"Error inserting data for {data['symbol']}: {e}")
            return False
    
    def process_all_companies(self, delay: float = 0.5):
        """Main method to process all companies"""
        if not self.connect_db():
            return
        
        if not self.create_table():
            return
        
        companies = self.get_company_symbols()
        total = len(companies)
        successful = 0
        failed = 0
        
        print(f"\nStarting to process {total} companies...")
        
        for idx, (symbol, name) in enumerate(companies, 1):
            print(f"\nProcessing {idx}/{total}: {name} ({symbol})")
            
            data = self.fetch_company_data(symbol)
            
            if data:
                if self.insert_company_data(data):
                    successful += 1
                    print(f"✓ Successfully inserted data for {symbol}")
                else:
                    failed += 1
            else:
                failed += 1
            
            # Add delay to avoid rate limiting
            time.sleep(delay)
        
        print(f"\n{'='*50}")
        print(f"Processing complete!")
        print(f"Total companies: {total}")
        print(f"Successful: {successful}")
        print(f"Failed: {failed}")
        print(f"{'='*50}")
        
        self.close_connection()
    
    def close_connection(self):
        """Close database connection"""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            print("\nDatabase connection closed")


# Main execution
if __name__ == "__main__":
    # Database configuration
    DB_CONFIG = {
        'host': os.getenv('db_host'),          # Change to your host
        'database': 'india_listed_companies',  # Change to your database name
        'user': os.getenv('db_user'),      # Change to your username
        'password': os.getenv('db_password')   # Change to your password
    }
    
    # Create extractor instance
    extractor = CompanyDataExtractor(
        host=DB_CONFIG['host'],
        database=DB_CONFIG['database'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password']
    )
    
    # Process all companies
    extractor.process_all_companies(delay=1)  # 1 second delay between requests