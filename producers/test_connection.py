from confluent_kafka.admin import AdminClient
from confluent_kafka.schema_registry import SchemaRegistryClient

from producers.config import kafka_config, sr_config


admin = AdminClient(kafka_config())
md = admin.list_topics()
print(f"Topics: {md.topics}")