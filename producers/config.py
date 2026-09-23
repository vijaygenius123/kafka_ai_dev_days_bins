import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

TOPIC_REGISTRY = "bin_registry"
TOPIC_TELEMETRY = "bin_telemetry"
TOPIC_FOOTFALL = "footfall_counts"
TOPIC_WEATHER = "weather_obs"
TOPIC_ENRICHED = "bin_events_enriched"
TOPIC_DISPATCH = "dispatch_list"


def _require(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Environment variable {name} is required")
    return value


BOOTSTRAP_SERVERS = _require("BOOTSTRAP_SERVERS")
KAFKA_API_KEY = _require("KAFKA_API_KEY")
KAFKA_API_SECRET = _require("KAFKA_API_SECRET")
SCHEMA_REGISTRY_URL = _require("SCHEMA_REGISTRY_URL")
SR_API_KEY = _require("SR_API_KEY")
SR_API_SECRET = _require("SR_API_SECRET")

SCHEMA_DIR = Path(__file__).parent / "schemas"


def kafka_config(**overrides) -> dict:
    cfg = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": KAFKA_API_KEY,
        "sasl.password": KAFKA_API_SECRET,
    }
    cfg.update(overrides)
    return cfg


def sr_config() -> dict:
    return {
        "url": SCHEMA_REGISTRY_URL,
        "basic.auth.user.info": f"{SR_API_KEY}:{SR_API_SECRET}",
    }


def load_schema(name: str) -> str:
    return (SCHEMA_DIR / f"{name}.avsc").read_text()
