from confluent_kafka import Consumer, KafkaError

def consume_data(topic):
    consumer = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'consumer_group_1',
        'auto.offset.reset': 'earliest'
    })

    consumer.subscribe([topic])

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    continue
                else:
                    print(f"Consumer error: {msg.error()}")
                    break

            # Process the message
            print(f"Consumer 1 received message: {msg.value().decode('utf-8')}")

    finally:
        consumer.close()

if __name__ == "__main__":
    kafka_topic = 'topic1'
    consume_data(kafka_topic)
