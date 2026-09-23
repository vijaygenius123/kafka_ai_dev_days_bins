import time

from producers.config import TOPIC_FOOTFALL
from producers.kafka_io import make_producer, make_serializer, send
from producers.sim import footfall_records


def main():
    producer = make_producer()
    serializer = make_serializer("footfall_counts")
    sim_minute = 480
    try:
        while True:
            for rec in footfall_records(sim_minute): send(producer, serializer, TOPIC_FOOTFALL, rec["district_id"], rec)
            sim_minute = (sim_minute + 1) % 1440
            time.sleep(1.0)
    except KeyboardInterrupt:
        print("\nflushing...")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
