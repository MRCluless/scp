import json
from kafka import KafkaConsumer

def main():
    consumer = KafkaConsumer(
        'f1',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='batch-lake-writer',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    for message in consumer:
        print(message.value)

if __name__ == "__main__":
    main()