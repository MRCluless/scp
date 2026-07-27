import json
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import KafkaSource
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.functions import MapFunction, AggregateFunction
from pyflink.datastream.window import SlidingProcessingTimeWindows, Time
from pyflink.common import Row
from pyflink.common.typeinfo import Types

class ParseTelemetry(MapFunction):
    def map(self, value):
        data = json.loads(value)
        return Row(
            Driver=data.get('Driver', 'Unknown'),
            Time=data.get('Time'),
            Speed=float(data.get("Speed", 0.0)),
            RPM=int(data.get("RPM", 0))
        )

class AverageTelemetry(AggregateFunction):
    def create_accumulator(self):
        return (0.0, 0.0, 0.0)
    
    def add(self, value, accumulator):
        return (accumulator[0] + value.Speed, accumulator[1] + value.RPM, accumulator[2] + 1)

    def get_result(self, accumulator):
        if accumulator[2] == 0:
            return Row(AvgSpeed=0.0, AvgRPM=0.0)
        return Row(
            AvgSpeed=accumulator[0] / accumulator[2], 
            AvgRPM=accumulator[1] / accumulator[2]
        )
    
    def merge(self, a, b):
        return (a[0] + b[0], a[1] + b[1], a[2] + b[2])
    
def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars("file:///Users/mohit/Desktop/Bhavik/scp/flink/flink-sql-connector-kafka-5.0.0-2.2.jar")
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(1)

    telemetry_schema = Types.ROW_NAMED(
        field_names=["Driver", "Time", "Speed", "RPM"],
        field_types=[Types.STRING(), Types.STRING(), Types.FLOAT(), Types.INT()]
    )

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers("localhost:9092")
        .set_topics("f1")
        .set_group_id("flink-consumer")
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    ds = env.from_source(source=source, watermark_strategy=WatermarkStrategy.no_watermarks(), source_name="Kafka-source")
    parsed_ds = ds.map(ParseTelemetry(), output_type=telemetry_schema)

    windowed_ds = parsed_ds \
        .key_by(lambda row: row.Driver, key_type=Types.STRING()) \
        .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(2))) \
        .aggregate(
            AverageTelemetry(),
            accumulator_type=Types.TUPLE([Types.FLOAT(), Types.FLOAT(), Types.INT()]),
            output_type=Types.ROW_NAMED(["AvgSpeed", "AvgRPM"], [Types.FLOAT(), Types.FLOAT()])
        )
    windowed_ds.print()

    env.execute("Kafka Telemetry Sliding Window Job")

if __name__ == "__main__":
    main()