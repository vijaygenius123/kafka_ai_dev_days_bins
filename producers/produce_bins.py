import time

from producers.config import TOPIC_TELEMETRY
from producers.kafka_io import make_producer, make_serializer, send
from producers.sim import tick

START_MINUTE = 480


def main():
    producer = make_producer()
    serializer = make_serializer("bin_telemetry")
    sim_minute = START_MINUTE
    sent = 0
    try:
        while True:
            for rec in tick(sim_minute):
                send(producer, serializer, TOPIC_TELEMETRY, rec["bin_id"], rec)
                sent += 1
            if sim_minute % 30 == 0:
                print(f"sim {sim_minute // 60:02d}:{sim_minute % 60:02d}  sent={sent}", flush=True)
            sim_minute = (sim_minute + 1) % 1440
            time.sleep(1.0)
    except KeyboardInterrupt:
        print("\nflushing...")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
