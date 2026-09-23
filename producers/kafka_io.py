from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField, StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from producers.config import kafka_config, sr_config, load_schema

_sr = SchemaRegistryClient(sr_config())
_key_ser = StringSerializer("utf_8")


def make_serializer(schema_name: str) -> AvroSerializer:
    """schema_name is the .avsc filename without extension."""
    return AvroSerializer(_sr, load_schema(schema_name), lambda obj, ctx: obj)


def make_producer() -> Producer:
    return Producer(kafka_config())


def delivery_report(err, msg):
    if err is not None: print(f"FAILED: {err}")


def send(producer, serializer, topic, key, record, on_delivery=delivery_report):
    producer.produce(topic=topic, key=_key_ser(key, SerializationContext(topic, MessageField.KEY)),
                     value=serializer(record, SerializationContext(topic, MessageField.VALUE)),
                     on_delivery=on_delivery, )
    producer.poll(0)
