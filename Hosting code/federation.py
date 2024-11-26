from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

class DatabaseFederator:
    def __init__(self, username, password, host, port, db_name, db_type = 'sqlite'):
        self.db_type = db_type
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.db_name = db_name
        self.engine = None
        self.connected = False

    def connect(self):
        """
        Establish a connection to the specified database.
        """
        try:
            if self.db_type == 'sqlite':
                # SQLite connection string
                connection_url = f"sqlite:///{self.db_name}.db"
            else:
                # General connection string
                connection_url = f"{self.db_type}://{self.username}:{self.password}@{self.host}:{self.port}/{self.db_name}"
            
            # Create the engine
            self.engine = create_engine(connection_url)
            
            # Test connection
            with self.engine.connect() as connection:
                connection.execute(text("SELECT 1"))
            
            self.connected = True
            print(f"Connected to database '{self.db_name}' successfully.")
        
        except SQLAlchemyError as e:
            self.connected = False
            print(f"Failed to connect to database '{self.db_name}': {e}")

    def extract_data(self, table_name, condition=None):
        if not self.connected:
            print("Not connected to the database. Call connect() first.")
            return None

        try:
            with self.engine.connect() as connection:
                query = f"SELECT * FROM {table_name}"
                if condition:
                    query += f" WHERE {condition}"

                result = connection.execute(text(query))
                data = [dict(row) for row in result]
            
            return data

        except SQLAlchemyError as e:
            print(f"Error extracting data from table '{table_name}': {e}")
            return None

    def close_connection(self):
        """
        Close the database connection.
        """
        if self.engine:
            self.engine.dispose()
            self.connected = False
            print(f"Connection to database '{self.db_name}' closed.")

# Example Usage
if __name__ == "__main__":
    # Create a federator for a MySQL database
    federator = DatabaseFederator(
        # db_type="mysql",
        username="root",
        password="password123",
        host="127.0.0.1",
        port=3306,
        db_name="test_db"
    )

    federator.connect()
    if federator.connected:
        data = federator.extract_data(table_name="users")
        print("All Data:", data)
    
        filtered_data = federator.extract_data(table_name="users", condition="age > 25")
        print("Filtered Data:", filtered_data)
    
    federator.close_connection()