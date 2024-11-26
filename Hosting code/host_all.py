from threading import Thread
from host_db_module import host_db

def create_and_run_app(db_name, host, port):
    app = host_db(
        username='', 
        password='', 
        host='', 
        port=0, 
        db_name=db_name
    )
    app.run(host=host, port=port)

def host_multiple_databases(db_names, base_host='127.0.0.', starting_port=5000):
    threads = [] 

    for idx, db_name in enumerate(db_names):
        host = f"{base_host}{idx + 1}" 
        port = starting_port + idx  

        thread = Thread(target=create_and_run_app, args=(db_name, host, port))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()


if __name__ == "__main__":
    # change this naams
    database_list = ["Irrigated_Area_and_Crop_Price", "soil_heath", "weather_data"]
    host_multiple_databases(database_list, starting_port=5000)