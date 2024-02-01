import requests
import json
import time
from confluent_kafka import Producer

# Kafka configuration
kafka_bootstrap_servers = 'localhost:9092'
kafka_topic = 'shots'

# API URL
api_url = 'http://127.0.0.1:5000/api/matches'

# Kafka producer configuration
kafka_config = {
    'bootstrap.servers': kafka_bootstrap_servers,
    'client.id': 'shots-producer'
}

# Function to send item to Kafka
def send_to_kafka(item):
    producer = Producer(kafka_config)
    try:
        # Produce JSON-encoded message to Kafka topic
        producer.produce(kafka_topic, json.dumps(item).encode('utf-8'))
        producer.flush()
        print(f'Item with the id {item["id"]} sent to Kafka')
    except Exception as e:
        print(f'Error sending item to Kafka: {e}')
    finally:
        producer.poll(0)


# Function to fetch data from API and send to Kafka with a delay
def process_data():
    try:
        response = requests.get(api_url)
        if response.status_code == 200:
            data = response.json()

            for item in data:
                send_to_kafka(item)
                time.sleep(3)
        else:
            print(f'Error fetching data from API. Status code: {response.status_code}')
    except Exception as e:
        print(f'Error processing data: {e}')

# Call the function to start the process
process_data()