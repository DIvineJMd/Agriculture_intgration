import zmq
import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Where
from sqlparse.tokens import Keyword, DML
from rich.console import Console
from rich.table import Table
from federator import DatabaseFederator

class QueryGen:
    def __init__(self, server_configs):
        """
        Initialize the federator with server configurations.
        Creates an instance of DatabaseFederator for each server.
        """
        self.server_instances = [
            {"db_name": server["db_name"], "federator": DatabaseFederator([server])}
            for server in server_configs
        ]
        self.console = Console()

    def query_servers(self, table, query):
        """
        Send a query to all relevant servers for the specified table.
        """
        data_from_servers = []
        for server in self.server_instances:
            self.console.print(f"[bold blue]Checking server: {server['db_name']}[/bold blue]")
            federator = server["federator"]
            data = federator.query_server(server_configs[0], query)  # Use server_configs[0] as input
            if data:
                data_from_servers.extend(data)
        return data_from_servers

    def parse_query(self, query):
        """
        Parse the SQL query to extract components like SELECT, FROM, and WHERE.
        """
        parsed = sqlparse.parse(query)[0]
        tokens = parsed.tokens

        # Extract query components
        query_components = {"select": [], "from": [], "where": None}
        current_keyword = None

        for token in tokens:
            if token.ttype is Keyword.DML and token.value.upper() == "SELECT":
                current_keyword = "SELECT"
            elif token.ttype is Keyword and token.value.upper() == "FROM":
                current_keyword = "FROM"
            elif current_keyword == "SELECT":
                if isinstance(token, IdentifierList):
                    query_components["select"].extend([str(t) for t in token.get_identifiers()])
                elif isinstance(token, Identifier):
                    query_components["select"].append(str(token))
            elif current_keyword == "FROM":
                if isinstance(token, IdentifierList):
                    query_components["from"].extend([str(t) for t in token.get_identifiers()])
                elif isinstance(token, Identifier):
                    query_components["from"].append(str(token))
            elif isinstance(token, Where):
                query_components["where"] = str(token)

        return query_components

    def federate_data(self, query_components):
        """
        Federate data from multiple servers based on the FROM clause.
        """
        data_from_servers = []
        for table in query_components["from"]:
            federated_data = self.query_servers(table, f"SELECT * FROM {table}")
            if federated_data:
                data_from_servers.extend(federated_data)
        return data_from_servers

    def perform_query(self, data, query_components):
        """
        Perform the query operations such as selection, projection, and filtering.
        """
        if not data:
            self.console.print("[bold yellow]No data available to perform query.[/bold yellow]")
            return []

        # Perform projection (SELECT columns)
        projected_data = [
            {col: row[col] for col in query_components["select"] if col in row}
            for row in data
        ]

        # Perform filtering (WHERE conditions)
        if query_components["where"]:
            condition = query_components["where"]
            filtered_data = [
                row for row in projected_data if eval(condition.replace("=", "=="), {}, row)
            ]
        else:
            filtered_data = projected_data

        return filtered_data

    def display_data(self, data):
        """
        Display the extracted data in a table format.
        """
        if not data:
            self.console.print("[bold yellow]No data available to display.[/bold yellow]")
            return

        table = Table(show_header=True, header_style="bold magenta")
        for column in data[0].keys():
            table.add_column(column)
        for row in data:
            table.add_row(*map(str, row.values()))
        self.console.print(table)

    def execute_query(self, query):
        """
        Parse the query, federate data, and execute the query.
        """
        self.console.print("[bold blue]Parsing query...[/bold blue]")
        query_components = self.parse_query(query)

        self.console.print("[bold blue]Federating data from servers...[/bold blue]")
        federated_data = self.federate_data(query_components)

        self.console.print("[bold blue]Executing query...[/bold blue]")
        result_data = self.perform_query(federated_data, query_components)

        self.console.print("[bold blue]Displaying results...[/bold blue]")
        self.display_data(result_data)


server_configs = [
    {"host": "127.0.0.1", "port": 5555, "db_name": "crop_prices", "tables": ["crop_prices"]},
    {"host": "127.0.0.1", "port": 5556, "db_name": "crop_data", "tables": ["crop_data"]},
    # rest
]


if __name__ == "__main__":
    federator = QueryGen(server_configs)
    print("Query Generator started. Enter 'exit' to quit.")

    while True:
        
        query = input("Enter your Query: ")
        if query=='exit':
            print("Exiting...")
            break
        federator.execute_query(query)