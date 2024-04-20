import json
from confluent_kafka import Producer

def delivery_callback(err, msg):
    if err:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to topic {msg.topic()} partition {msg.partition()}")

def produce_data(file_path, topic):
    producer = Producer({'bootstrap.servers': 'localhost:9092', 'queue.buffering.max.messages': 1000000})

    with open(file_path, 'r') as file:
        for line in file:
            # Produce each line as a message to Kafka topic
            producer.produce(topic, value=line.rstrip(), callback=delivery_callback)

    # Wait for all messages to be delivered
    producer.flush()

if __name__ == "__main__":
    file_path = r'C:\Users\abdul\OneDrive\Desktop\cleaned2.json'

    kafka_topic = 'topic1'
    produce_data(file_path, kafka_topic)
