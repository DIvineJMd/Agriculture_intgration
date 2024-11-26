from federator import DatabaseFederator
# from cli import cli

if __name__ == "__main__":
    servers = [
        {"host": "127.0.0.1", "port": 5555, "db_name": "crop_prices"},
        # {"host": "192.168.1.10", "port": 5555, "db_name": "another_db"},
    ]

    federator = DatabaseFederator(servers)

    while True:
        try:
            ###########################
            # Cli YAHA DAL. niche vala hata de
            ###########################
            command = input("Enter command (query/exit): ").strip().lower()
            if command == "exit":
                break

            if command == "query":
                # Choose a server
                for idx, server in enumerate(servers):
                    print(f"[{idx}] {server['db_name']} at {server['host']}:{server['port']}")
                server_idx = int(input("Select a server by index: ").strip())
                selected_server = servers[server_idx]

                # Query input
                query = input("Enter SQL query: ").strip()

                # Query the selected server
                data = federator.query_server(selected_server, query)

                # Display the results
                federator.display_data(data)

        except KeyboardInterrupt:
            print("\nExiting...")
            break
        except ValueError:
            print("Invalid input. Please try again.")
