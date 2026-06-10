## Spark Structured Streaming Assignment

This assignment demonstrates Spark Structured Streaming using a simulated ride-sharing data stream. The stream is generated with Python and sent through a socket on `localhost:9999`.

## Files

- `data_generator.py`: Generates simulated ride data in JSON format.
- `task1.py`: Reads streaming data from the socket, parses the JSON, and displays the structured data.
- `task2.py`: Aggregates total fare amount and average distance by driver.
- `task3.py`: Performs a 5-minute windowed aggregation with a 1-minute slide and watermarking.

## Data Fields

Each generated ride record contains:

- `trip_id`
- `driver_id`
- `distance_km`
- `fare_amount`
- `timestamp`

## How to Run

Start the data generator first:

```bash
python data_generator.py