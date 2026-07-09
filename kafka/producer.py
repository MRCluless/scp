import time
import json
import fastf1
from kafka import KafkaProducer
import pandas as pd

fastf1.Cache.enable_cache("../.f1cache")
session = fastf1.get_session(2026, "Barcelona", 'R')
session.load(telemetry=True, laps=True)
laps = session.laps.pick_drivers(["HAM"])
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
    for idx, lap in laps.iterrows():
        telemetry = lap.get_telemetry()
        lap_number = int(lap["LapNumber"])
        for i in range(len(telemetry)):
            curr = telemetry.iloc[i]
            row_dict = curr.to_dict()
            row_dict["Lap"] = lap_number
            producer.send("hamilton", row_dict)
            if i < len(telemetry) - 1:
                next_row = telemetry.iloc[i + 1]
                if "Time" in curr and "Time" in next_row:
                    time_delta = (
                        next_row["Time"] - curr["Time"]
                    ).total_seconds()
                    if time_delta > 0:
                        time.sleep(time_delta)
except KeyboardInterrupt:
    print("Stopped Producer")
finally:
    print("Finished Simulating F1 data.")
    producer.close()