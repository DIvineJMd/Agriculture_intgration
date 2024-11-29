import sqlite3
import zmq
import logging
from datetime import datetime
import json

# Logging setup
logging.basicConfig(
    filename="server_logs.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

#  badal de ye, ports sabki unique rakhio
DB_FILE = "database/macro_nutrients.db"
ZMQ_PORT = 5555

def log_message(message):
    """Log a message with timestamp."""
    logging.info(message)
    print(message)  # Print to console for real-time monitoring

def execute_query(sql_query):
    """Execute a query on the SQLite database and return the results."""
    try:
        # Connect to the SQLite database
        connection = sqlite3.connect(DB_FILE)
        cursor = connection.cursor()
        cursor.execute(sql_query)
        
        # Fetch results and column names
        data = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        
        connection.close()
        print(f"Query executed successfully: {sql_query}")  # Log the query
        return {"data": [dict(zip(columns, row)) for row in data], "error": None}
    except sqlite3.Error as e:
        print(f"Database error: {e}")  # Log the error
        return {"data": None, "error": str(e)}

def start_server():
    """
    Start a ZeroMQ server that listens for incoming SQL queries.
    """
    context = zmq.Context()
    socket = context.socket(zmq.REP)  # REP: Response socket
    socket.bind(f"tcp://0.0.0.0:{ZMQ_PORT}")
    log_message(f"Server started on port {ZMQ_PORT}")

    while True:
        try:
            # Wait for a request from the client
            message = socket.recv_json()
            log_message(f"Received message: {message}")

            if "query" not in message:
                response = {"results": None, "error": "Missing 'query' field in request"}
                socket.send_json(response)
                log_message("Sent response: Missing 'query' field")
                continue

            sql_query = message["query"]
            # Execute the query and prepare the response
            response = execute_query(sql_query)
            socket.send_json(response)
            log_message(f"Sent response: {response}")

        except Exception as e:
            error_message = f"Server error: {str(e)}"
            log_message(error_message)
            socket.send_json({"results": None, "error": error_message})

if __name__ == "__main__":
    log_message("Starting database server...")
    start_server()