import json
import fastf1
from kafka import KafkaProducer
import pandas as pd
fastf1.Cache.enable_cache("../.f1cache")
class CustomF1Encoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, pd.Timedelta):
            return str(obj)
        
        if isinstance(obj, pd.Timestamp):
            return obj.isoformat()

        return super().default(obj)

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x, cls=CustomF1Encoder, indent=4).encode('utf-8'),
)

try:
    for RACE in range(1,10):
        session = fastf1.get_session(2026, RACE, "R")
        session.load(laps=True, telemetry=True)
        laps = session.laps
        for idx, lap in laps.iterrows():
            telemetry = lap.get_telemetry()
            lap_number = int(lap["LapNumber"])
            driver = lap["Driver"]
            for i in range(len(telemetry)):
                curr = telemetry.iloc[i]
                row_dict = curr.to_dict()
                row_dict["Lap"] = lap_number
                row_dict["Driver"] = driver
                producer.send("f1", key=str(driver).encode('utf-8'), value=row_dict)
except Exception as e:
    print("Stopped Producer")
finally:
    print("Finished Simulating F1 data.")
    producer.flush()
    producer.close()