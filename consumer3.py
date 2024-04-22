from kafka import KafkaConsumer
import json

# Initialize Kafka consumer
consumer = KafkaConsumer('topic1',
                         bootstrap_servers=['localhost:9092'],
                         auto_offset_reset='earliest',
                         group_id='group3')

# Process messages
for message in consumer:
    data = json.loads(message.value)
    # Process the data received from Kafka
    print("Consumer 3 received:", data)

# Close the consumer
consumer.close()
