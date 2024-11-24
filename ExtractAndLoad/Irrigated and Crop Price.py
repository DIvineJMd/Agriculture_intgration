import pandas as pd
import sqlite3
from datetime import datetime
import os

def clean_column_name(name):
    """Clean column names to make them SQL friendly"""
    return name.replace(' ', '_').replace('(', '').replace(')', '').replace('/', '_').replace('x0020_', '')

def convert_date(date_str):
    """Convert date string to ISO format"""
    try:
        return datetime.strptime(date_str, '%d/%m/%Y').strftime('%Y-%m-%d')
    except:
        return None

def load_crop_prices():
    """Load crop prices data into SQLite database"""
    # Read CSV file
    df = pd.read_csv('rawData/Crop_prices.csv')
    
    # Clean column names
    df.columns = [clean_column_name(col) for col in df.columns]
    
    # Convert date
    df['Arrival_Date'] = df['Arrival_Date'].apply(convert_date)
    
    return df

def load_irrigated_area():
    """Load irrigated area data into SQLite database"""
    # Read CSV file
    df = pd.read_csv('rawData/IrrigatedArea.csv')
    
    # Clean column names
    df.columns = [clean_column_name(col) for col in df.columns]
    
    # Replace -1.0 with NULL
    df = df.replace(-1.0, None)
    
    return df

def create_database():
    """Create SQLite database and load data"""
    # Create database directory if it doesn't exist
    os.makedirs('database', exist_ok=True)
    
    # Update connection path to use database folder
    conn = sqlite3.connect('database/Irrigated_Area_and_Crop_Price.db')
    
    try:
        # Load dataframes
        crop_prices_df = load_crop_prices()
        irrigated_area_df = load_irrigated_area()
        
        # Save to database
        crop_prices_df.to_sql('crop_prices', conn, if_exists='replace', index=False)
        irrigated_area_df.to_sql('irrigated_area', conn, if_exists='replace', index=False)
        
        # Create indexes for better query performance
        conn.execute('CREATE INDEX IF NOT EXISTS idx_crop_prices_date ON crop_prices(Arrival_Date)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_irrigated_area_year ON irrigated_area(Year)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_crop_prices_state ON crop_prices(State)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_irrigated_area_state ON irrigated_area(State_Name)')
        
        print("Database created successfully!")
        
    except Exception as e:
        print(f"Error creating database: {str(e)}")
        
    finally:
        conn.close()

if __name__ == "__main__":
    create_database()
