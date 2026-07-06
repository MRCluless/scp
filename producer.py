import time
import json
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8'),
)

try:
    count = 0
    while True:
        data = {
            "count": count,
            "value": count
        }
        producer.send("topic1",data)
        count += 1
        time.sleep(1)
except KeyboardInterrupt:  
    print("Stopping Producer")
finally:
    producer.close()