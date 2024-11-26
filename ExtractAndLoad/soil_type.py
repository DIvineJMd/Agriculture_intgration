import pandas as pd
import sqlite3
import os

def clean_column_name(name):
    """Clean column names to make them SQL friendly"""
    return name.replace(' ', '_').replace('.', '').replace('(', '').replace(')', '').lower()

def load_soil_types():
    """Load soil types data into SQLite database"""
    try:
        # Read CSV file
        df = pd.read_csv('rawData/soil_types_india.csv')
        
        # Clean column names
        df.columns = [clean_column_name(col) for col in df.columns]
        
        # Split the 'found_in' column into a separate states table
        states_data = []
        for idx, row in df.iterrows():
            states = [state.strip() for state in row['found_in'].split(',')]
            for state in states:
                states_data.append({
                    'soil_type_id': idx + 1,  # Adding 1 because idx starts at 0
                    'state': state
                })
        
        states_df = pd.DataFrame(states_data)
        
        return df, states_df
    
    except Exception as e:
        print(f"Error loading soil types data: {str(e)}")
        return None, None

def create_database():
    """Create SQLite database for soil types"""
    try:
        # Create database directory if it doesn't exist
        os.makedirs('database', exist_ok=True)
        
        # Connect to database
        conn = sqlite3.connect('database/soil_types.db')
        cursor = conn.cursor()
        
        # Create soil_types table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS soil_types (
            id INTEGER PRIMARY KEY,
            soil_type TEXT NOT NULL,
            facts TEXT
        )
        ''')
        
        # Create soil_type_states table for many-to-many relationship
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS soil_type_states (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            soil_type_id INTEGER,
            state TEXT NOT NULL,
            FOREIGN KEY (soil_type_id) REFERENCES soil_types (id),
            UNIQUE (soil_type_id, state)
        )
        ''')
        
        # Load data
        soil_types_df, states_df = load_soil_types()
        
        if soil_types_df is not None and states_df is not None:
            # Insert soil types data
            soil_types_data = soil_types_df[['slno', 'soil_type', 'facts']].rename(columns={'slno': 'id'})
            soil_types_data.to_sql('soil_types', conn, if_exists='replace', index=False)
            
            # Insert states data
            states_df.to_sql('soil_type_states', conn, if_exists='replace', index=False)
            
            # Create indexes for better query performance
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_soil_type_states_state ON soil_type_states(state)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_soil_type_states_soil_type_id ON soil_type_states(soil_type_id)')
            
            print("Soil types database created successfully!")
            
            # Verify the data
            cursor.execute("SELECT COUNT(*) FROM soil_types")
            soil_types_count = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM soil_type_states")
            states_count = cursor.fetchone()[0]
            
            print(f"Inserted {soil_types_count} soil types and {states_count} state associations")
        
        conn.commit()
        conn.close()
        
    except Exception as e:
        print(f"Error creating soil types database: {str(e)}")

def query_soil_types_by_state(state):
    """Helper function to query soil types for a specific state"""
    try:
        conn = sqlite3.connect('database/soil_types.db')
        cursor = conn.cursor()
        
        cursor.execute('''
        SELECT st.soil_type, st.facts 
        FROM soil_types st
        JOIN soil_type_states sts ON st.id = sts.soil_type_id
        WHERE sts.state = ?
        ''', (state,))
        
        results = cursor.fetchall()
        conn.close()
        
        return results
    
    except Exception as e:
        print(f"Error querying soil types: {str(e)}")
        return []

if __name__ == "__main__":
    # Create the database and load data
    create_database()
    
    # Example query to verify the data
    print("\nExample Query - Soil Types in Maharashtra:")
    results = query_soil_types_by_state("Maharashtra")
    for soil_type, facts in results:
        print(f"\nSoil Type: {soil_type}")
        print(f"Facts: {facts}")