from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode, MapFunction
from pyflink.datastream.connectors.kafka import KafkaSource
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.functions import MapFunction
from pyflink.common import Row
from pyflink.common.typeinfo import Types 
import json

class ParseTelemetry(MapFunction):
    def map(self, value):
        data = json.loads(value)
        return Row(
            Time=data.get("Time"),
            Speed=float(data.get("Speed", 0.0)),
            RPM=int(data.get("RPM", 0)),
            Gear=int(data.get("Gear", 0)),
            Throttle=float(data.get("Throttle", 0.0)),
            Brake=float(data.get("Brake", 0.0))
        )

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars("file:///Users/mohit/Desktop/Bhavik/scp/flink/flink-sql-connector-kafka-5.0.0-2.2.jar")
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(1)

    telemetry_schema = Types.ROW_NAMED(
        field_names=["Time", "Speed", "RPM", "Gear", "Throttle", "Brake"],
        field_types=[
            Types.STRING(), 
            Types.FLOAT(), 
            Types.INT(), 
            Types.INT(), 
            Types.FLOAT(), 
            Types.FLOAT()
        ]
    )

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers("localhost:9092")
        .set_topics("hamilton")
        .set_group_id("flink-consumer")
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    ds = env.from_source(source=source, watermark_strategy=WatermarkStrategy.no_watermarks(), source_name="Kafka-source")
    parsed_ds = ds.map(ParseTelemetry(), output_type=telemetry_schema)
    parsed_ds.print()

    env.execute("Kafka Telemetry Print Job")

if __name__ == "__main__":
    main()