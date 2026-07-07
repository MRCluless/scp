import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'hamilton',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='group1',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

try:
    for message in consumer:
        print(f"Received: {message.value} from partition {message.partition}")
except KeyboardInterrupt:
    print("Stopping consumer...")
finally:
    consumer.close()