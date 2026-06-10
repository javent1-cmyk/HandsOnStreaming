import socket
import json
import time
import random
from faker import Faker
from datetime import datetime

fake = Faker()

HOST = "localhost"
PORT = 9999

server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind((HOST, PORT))
server_socket.listen(1)

print(f"Listening on {HOST}:{PORT}...")
conn, addr = server_socket.accept()
print(f"Connected by {addr}")

while True:
    ride = {
        "trip_id": fake.uuid4(),
        "driver_id": f"driver_{random.randint(1, 5)}",
        "distance_km": round(random.uniform(1.0, 30.0), 2),
        "fare_amount": round(random.uniform(5.0, 100.0), 2),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    conn.send((json.dumps(ride) + "\n").encode("utf-8"))
    print(ride)
    time.sleep(2)