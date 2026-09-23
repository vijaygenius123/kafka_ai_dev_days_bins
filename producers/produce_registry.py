from producers.config import TOPIC_REGISTRY
from producers.kafka_io import make_producer, make_serializer, send
from producers.sim import BINS


def main():
    producer = make_producer()
    serializer = make_serializer("bin_registry")
    for b in BINS: send(producer, serializer, TOPIC_REGISTRY, b["bin_id"],
                        {"bin_id": b["bin_id"], "district_id": b["district_id"], "route_id": b["route_id"],
                         "capacity_litres": int(b["capacity_litres"]), })
    producer.flush()
    print(f"Produced {len(BINS)} records to {TOPIC_REGISTRY}")


if __name__ == "__main__":
    main()
