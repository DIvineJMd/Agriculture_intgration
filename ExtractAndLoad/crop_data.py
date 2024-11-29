import sqlite3
import pandas as pd

def load_csv_to_db(csv_file, db_file):
    # Read the CSV file into a DataFrame
    data = pd.read_csv(csv_file)
    
    # Connect to the SQLite database (or create it if it doesn't exist)
    conn = sqlite3.connect(db_file)
    
    # Create a table for the crop data
    conn.execute('''
    CREATE TABLE IF NOT EXISTS crop_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nitrogen FLOAT,
        phosphorus FLOAT,
        potassium FLOAT,
        temperature FLOAT,
        humidity FLOAT,
        ph FLOAT,
        rainfall FLOAT,
        label TEXT
    )
    ''')
    
    # Insert the data into the table
    data.to_sql('crop_data', conn, if_exists='replace', index=False)
    
    # Commit and close the connection
    conn.commit()
    conn.close()
    print("Data loaded successfully into the database.")

if __name__ == "__main__":
    csv_file = 'rawData/CropData.csv'
    db_file = 'database/crop_data.db'

    load_csv_to_db(csv_file, db_file)
    # load_csv_to_db(csv_file, 'Transformed_database/transformed_crop_data.db')
