from kafka import KafkaConsumer
import json
from pymongo import MongoClient

# Initialize MongoDB client and connect to the database
client = MongoClient('localhost', 27017)
db = client['mydatabase']
collection = db['consumer1_collection']

# Initialize Kafka consumer
consumer = KafkaConsumer('topic1',
                         bootstrap_servers=['localhost:9092'],
                         auto_offset_reset='earliest',
                         group_id='group1')

# Process messages
for message in consumer:
    data = json.loads(message.value)
    # Store data in MongoDB
    collection.insert_one(data)
    print("Consumer 1 stored data in MongoDB:", data)

# Close the consumer
consumer.close()
