from kafka import KafkaConsumer

def create_consumer():
    print("Connecting to Kafka brokers")
    consumer = KafkaConsumer(
        "bekusovmhw3",
        group_id="tumbling",
        bootstrap_servers="localhost:29092",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )
    for message in consumer:
        print(message)


if __name__ == "__main__":
    create_consumer()