from kafka import KafkaConsumer
import time
import random


def backoff(tries, sleep):
    def decorator(func):
        def wrapper(*args, **kwargs):
            for i in range(tries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if i < tries - 1:
                        time.sleep(sleep)
                    else:
                        raise e
        return wrapper
    return decorator


@backoff(tries=10, sleep=7)
def message_handler(value):
    if random.random()<0.5:
        print("retry")
        raise Exception
    print(value)


def create_consumer():
    print("Connecting to Kafka brokers")
    consumer = KafkaConsumer(
        "bekusovmhw3",
        group_id="itmo_group1",
        bootstrap_servers="localhost:29092",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )

    for message in consumer:
        message_handler(message)


if __name__ == "__main__":
    create_consumer()
