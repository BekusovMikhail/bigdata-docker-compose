from pyflink.common import SimpleStringSchema
from pyflink.common.typeinfo import Types, RowTypeInfo
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import (
    StreamExecutionEnvironment,
    TimeCharacteristic,
    CheckpointConfig,
)

# from pyflink.datastream.checkpoint_config import CheckpointConfig
from pyflink.datastream.connectors import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
    KafkaSink,
    KafkaRecordSerializationSchema,
)
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.datastream.functions import MapFunction


def python_data_stream_example():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    env.enable_checkpointing(5000)
    checkpoint_cfg = env.get_checkpoint_config()
    checkpoint_cfg.set_checkpoint_interval(5000)
    checkpoint_cfg.set_checkpoint_timeout(60000)
    checkpoint_cfg.set_max_concurrent_checkpoints(10)
    checkpoint_cfg.set_checkpoint_storage_dir(
        "file:///opt/pyflink/tmp/checkpoints/logs"
    )

    type_info: RowTypeInfo = Types.ROW_NAMED(
        ["device_id", "temperature", "execution_time"],
        [Types.LONG(), Types.DOUBLE(), Types.INT()],
    )

    json_row_schema = (
        JsonRowDeserializationSchema.builder().type_info(type_info).build()
    )

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers("kafka:9092")
        .set_topics("bekusovmhw3")
        .set_group_id("pyflink-e2e-source")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(json_row_schema)
        .build()
    )

    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers("kafka:9092")
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("bekusovmhw3processed")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )

    ds = env.from_source(
        source, WatermarkStrategy.no_watermarks(), "Kafka Source"
    )
    ds.map(TemperatureFunction(), Types.STRING()).sink_to(sink)
    env.execute_async("Devices preprocessing")


class TemperatureFunction(MapFunction):
    def map(self, value):
        device_id, temperature, execution_time = value
        return str(
            {
                "device_id": device_id,
                "temperature": temperature - 273,
                "execution_time": execution_time,
            }
        )


if __name__ == "__main__":
    python_data_stream_example()
