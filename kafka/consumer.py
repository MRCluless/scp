import json
from kafka import KafkaConsumer

def main():
    consumer = KafkaConsumer(
        'hamilton',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='batch-lake-writer',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    current_lap = None
    lap_data = []
    try:
        for message in consumer:
            data = message.value
            lap = data.get("Lap")

            if current_lap is not None and lap != current_lap:
                filename = f"Lap_{current_lap}.json"
                with open(f"../s3/{filename}", "w") as f:
                    json.dump(lap_data, f, indent=4)
                lap_data = []
            current_lap = lap
            lap_data.append(data)
    except KeyboardInterrupt:
            print("Batch Writer Stopped.")

if __name__ == "__main__":
    main()