from federator import DatabaseFederator
import os
# from cli import cli

if __name__ == "__main__":
    servers = [
        {"host": "35.222.106.199", "port": 1111, "db_name": "crop_prices"}
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

                import subprocess
                for folder in ["WareHouse", "database"]:
                    for filename in os.listdir(folder):
                        file_path = os.path.join(folder, filename)
                        if os.path.isfile(file_path):
                            os.remove(file_path) 
                subprocess.run(["python", "ExtractAndLoad/weather_data.py"])  # Execute weather_data.py
                subprocess.run(["python", "Transformation/weatherTransformation.py"])  
                subprocess.run(["python", "ExtractAndLoad/soilhealtdata.py"])  # Execute weather_data.py
                subprocess.run(["python", "Transformation/soilhealthTransformation.py"])  

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
