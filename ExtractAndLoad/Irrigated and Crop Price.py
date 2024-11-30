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

def create_databases():
    """Create separate SQLite databases for crop prices and irrigated area"""
    # Create database directory if it doesn't exist
    os.makedirs('database', exist_ok=True)
    
    # Create crop prices database
    crop_prices_conn = sqlite3.connect('database/crop_prices.db')
    try:
        # Load crop prices data
        crop_prices_df = load_crop_prices()
        
        # Save to database
        crop_prices_df.to_sql('crop_prices', crop_prices_conn, if_exists='replace', index=False)
        
        # Create indexes for better query performance
        crop_prices_conn.execute('CREATE INDEX IF NOT EXISTS idx_crop_prices_date ON crop_prices(Arrival_Date)')
        crop_prices_conn.execute('CREATE INDEX IF NOT EXISTS idx_crop_prices_state ON crop_prices(State)')
        
        print("[bold cyan]Crop prices database accessed successfully![\bold cyan]")
        
    except Exception as e:
        print(f"[bold red]Error creating crop prices database: {str(e)} [\bold red]")
    finally:
        crop_prices_conn.close()
    
    # Create irrigated area database
    irrigated_area_conn = sqlite3.connect('database/irrigated_area.db')
    try:
        # Load irrigated area data
        irrigated_area_df = load_irrigated_area()
        
        # Save to database
        irrigated_area_df.to_sql('irrigated_area', irrigated_area_conn, if_exists='replace', index=False)
        
        # Create indexes for better query performance
        irrigated_area_conn.execute('CREATE INDEX IF NOT EXISTS idx_irrigated_area_year ON irrigated_area(Year)')
        irrigated_area_conn.execute('CREATE INDEX IF NOT EXISTS idx_irrigated_area_state ON irrigated_area(State_Name)')
        
        print("[bold cyan]Irrigated area database created successfully![\bold cyan]")
        
    except Exception as e:
        print(f"[bold red]Error creating irrigated area database: {str(e)}[\bold red]")
    finally:
        irrigated_area_conn.close()

if __name__ == "__main__":
    create_databases()
