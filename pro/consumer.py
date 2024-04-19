from confluent_kafka import Producer, Consumer, KafkaError
import pandas as pd
import random
import json
import csv

data = pd.read_csv(r"C:\Chintan\practice\Ubereat_US_Merchant.csv")  # Ensure you specify the correct file path

unpicked_indices = list(data.index)
random.shuffle(unpicked_indices)

# Kafka broker configuration
bootstrap_servers = 'localhost:9092'
topic = 'my_topic'

# Kafka producer configuration
producer_config = {'bootstrap.servers': bootstrap_servers}

# Kafka consumer configuration
consumer_config = {
    'bootstrap.servers': bootstrap_servers,
    'group.id': 'my_consumer_group',
    'auto.offset.reset': 'earliest'
}


def get_unique_data(count=1):
    global unpicked_indices
    # Check if there are enough records left
    if count > len(unpicked_indices):
        count = len(unpicked_indices)
        if count == 0:
            return None  # No more data to return

    # Fetch random records
    selected_indices = unpicked_indices[:count]
    unpicked_indices = unpicked_indices[count:]

    # Convert records to JSON format
    records = [data.iloc[idx].to_dict() for idx in selected_indices]
    return json.dumps(records,indent=2)

def json_to_csv(data, csv_file):
    with open(csv_file, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["city", "state", "zipcode", "address", "scan_date"])  # Write header
        for entry in data:
            writer.writerow([entry["city"], entry["state"], entry["zipcode"], entry["address"], entry["scan_date"]])

    print("Data successfully written to", csv_file)

def produce_message(producer, topic, message):
    
    producer.produce(topic, message)
    producer.flush()
    

def consume_messages(consumer, topic):
    consumer.subscribe([topic])
    csv_file = r"C:\Chintan\practice\orders_data.csv"  # Ensure to use raw string (r"") for file paths
    data_file = []

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break

            print('Received message: {}'.format(msg.value().decode('utf-8')))

            json_data = json.loads(msg.value().decode('utf-8'))
            data_file.extend(json_data)  # Extend the list instead of appending each time
            # json_to_csv(json_data, csv_file)  # Move this line inside the loop if needed

            # Write data to CSV inside the loop
            with open(csv_file, mode='w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(["city", "state", "zipcode", "address", "scan_date"])  # Write header
                for entry in data_file:
                    writer.writerow([entry["city"], entry["state"], entry["zipcode"], entry["address"], entry["scan_date"]])

            print("Data successfully written to", csv_file)

    finally:
        consumer.close()


if __name__ == '__main__':
    # Create Kafka producer
    producer = Producer(**producer_config)

    # Create Kafka consumer
    consumer = Consumer(**consumer_config)

    try:
        # Produce messages
        while True:
            record = get_unique_data(1)
            if record is None:
                print("All unique records have been picked.")
                break
            print(record)

            produce_message(producer, topic, record)
        # produce_message(producer, topic, 'This is a message from Python!')

        # Consume messages
        consume_messages(consumer, topic)

    except KeyboardInterrupt:
        pass
    finally:
        # Close producer and consumer
        producer.flush()
        # producer.close()

