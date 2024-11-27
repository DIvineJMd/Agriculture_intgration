import zmq
from rich.console import Console
from rich.table import Table


class DatabaseFederator:
    def __init__(self, server_configs):
        """
        Initialize the DatabaseFederator with a list of server configurations.
        Each server config contains:
        {
            "host": "127.0.0.1",
            "port": 5555,
            "db_name": "example"
        }
        """
        self.server_configs = server_configs
        self.console = Console()

    def query_server(self, server, query):
        """
        Send a query to the server and retrieve the results.
        """
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        address = f"tcp://{server['host']}:{server['port']}"
        socket.connect(address)

        try:
            self.console.print(f"[bold cyan]Sending query to {server['db_name']} at {server['host']}...[/bold cyan]")
            socket.send_json({"query": query})

            # Wait for the response
            response = socket.recv_json()
            if response.get("error"):
                self.console.print(f"[bold red]Error: {response['error']}[/bold red]")
                return None
            self.console.print(f"[bold green]Data extracted from {server['db_name']} at {server['host']}.[/bold green]")
            return response["data"]
        except Exception as e:
            self.console.print(f"[bold red]Failed to query {server['db_name']} at {server['host']}: {e}[/bold red]")
            return None
        finally:
            socket.close()

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