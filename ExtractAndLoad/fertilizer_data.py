import pandas as pd
import sqlite3
from pathlib import Path

def create_fertilizer_database():
    # Create database directory if it doesn't exist
    db_dir = Path("database")
    db_dir.mkdir(exist_ok=True)
    
    # Read the CSV file
    csv_path = "rawData/Fertilizer Prediction.csv"
    df = pd.read_csv(csv_path)
    
    # Clean column names (remove spaces and standardize)
    df.columns = df.columns.str.strip().str.replace(' ', '_')
    
    # Create a connection to SQLite database in the database folder
    conn = sqlite3.connect(db_dir / 'fertilizer_prediction.db')
    
    try:
        # Create table and insert data
        df.to_sql('fertilizer_data', conn, if_exists='replace', index=False)
        print("Data successfully loaded into database!")
        
        # Verify the data
        result = conn.execute("SELECT COUNT(*) FROM fertilizer_data").fetchone()
        print(f"Total records in database: {result[0]}")
        
    except Exception as e:
        print(f"An error occurred: {e}")
        
    finally:
        conn.close()

if __name__ == "__main__":
    create_fertilizer_database()