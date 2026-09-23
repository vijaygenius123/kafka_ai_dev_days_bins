from confluent_kafka.admin import AdminClient, NewTopic

from producers.config import (
    kafka_config,
    TOPIC_REGISTRY,
    TOPIC_TELEMETRY,
    TOPIC_FOOTFALL,
)

TOPICS = [TOPIC_REGISTRY, TOPIC_TELEMETRY, TOPIC_FOOTFALL]


def main():
    admin = AdminClient(kafka_config())
    existing = set(admin.list_topics(timeout=10).topics)

    wanted = [t for t in TOPICS if t not in existing]
    for t in TOPICS:
        if t in existing:
            print(f"exists  {t}")

    if not wanted:
        return

    for topic, fut in admin.create_topics(
        [NewTopic(t, num_partitions=1, replication_factor=3) for t in wanted]
    ).items():
        try:
            fut.result()
            print(f"created {topic}")
        except Exception as e:
            print(f"FAILED  {topic}: {e}")


if __name__ == "__main__":
    main()
