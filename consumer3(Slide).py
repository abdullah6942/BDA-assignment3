from kafka import KafkaConsumer
import json
from collections import defaultdict, Counter
from itertools import combinations

def sliding_window_frequent_itemsets(data, window_size=5, min_support=2):
    # Initialize sliding window and frequent itemsets
    window = defaultdict(int)
    frequent_itemsets = Counter()

    # Process each item in the data stream
    for item in data:
        # Increment the count of the current item in the window
        window[item] += 1

        # Remove items that fall outside the sliding window
        if len(window) > window_size:
            old_item = data[len(data) - window_size - 1]
            window[old_item] -= 1
            if window[old_item] == 0:
                del window[old_item]

        # Generate item pairs and update frequent itemsets
        for pair in combinations(window.keys(), 2):
            frequent_itemsets[pair] += 1

    # Filter frequent itemsets by minimum support
    frequent_itemsets = {itemset: support for itemset, support in frequent_itemsets.items() if support >= min_support}
    return frequent_itemsets

def process_message(msg):
    try:
        data = json.loads(msg.value.decode('utf-8'))
        also_buy_data = data.get('also_buy', [])  # Extract 'also_buy' data or use an empty list if not present
        frequent_itemsets = sliding_window_frequent_itemsets(also_buy_data)
        print("Sliding Window frequent itemsets:", frequent_itemsets)
    except Exception as e:
        print(f"Error processing message: {e}")

def consume_data(topic):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        group_id='group3'
    )

    try:
        for message in consumer:
            process_message(message)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == "__main__":
    kafka_topic = 'topic1'  # Replace 'topic1' with your actual Kafka topic name for Consumer 3
    consume_data(kafka_topic)

