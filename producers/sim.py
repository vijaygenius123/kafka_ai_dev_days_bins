import math
import random
import time

DISTRICTS = [
    {"id": "D1", "name": "High Street", "mult": 1.8}, {"id": "D2", "name": "Station", "mult": 2.2},
    {"id": "D3", "name": "Park", "mult": 1.1}, {"id": "D4", "name": "Riverside", "mult": 0.6},
    {"id": "D5", "name": "Market Square", "mult": 1.5}, {"id": "D6", "name": "Docks", "mult": 0.4}, ]

DISTRICT_BY_ID = {d["id"]: d for d in DISTRICTS}

_CAPACITIES = [120, 240, 360, 240]
_ROUTE_1 = ("D1", "D2", "D3")

BINS = [
    {"bin_id": f"{d['id']}-B{n}", "district_id": d["id"], "route_id": "R1" if d["id"] in _ROUTE_1 else "R2",
     "capacity_litres": _CAPACITIES[n - 1], } for d in DISTRICTS for n in range(1, 5)]

_state = {
    b["bin_id"]: {"fill_pct": random.uniform(5.0, 25.0), "battery_pct": 100.0} for b in BINS}

# (centre_minute, width, weight) -- commute, lunch, evening
_PEAKS = [(510, 60, 1.0), (780, 90, 0.8), (1050, 75, 1.2)]


def _bump(x, centre, width):
    return math.exp(-((x - centre) ** 2) / (2 * width ** 2))


def footfall(sim_minute, mult):
    base = sum(w * _bump(sim_minute, c, width) for c, width, w in _PEAKS)
    return max(0.0, base * mult * 120.0 * random.uniform(0.85, 1.15))


def tick(sim_minute):
    """Advance every bin one sim-minute; return telemetry records."""
    now_ms = int(time.time() * 1000)
    records = []
    for b in BINS:
        st = _state[b["bin_id"]]
        mult = DISTRICT_BY_ID[b["district_id"]]["mult"]
        share = footfall(sim_minute, mult) / 4.0 * random.uniform(0.7, 1.3)
        st["fill_pct"] = min(100.0, st["fill_pct"] + share * 0.8 / b["capacity_litres"] * 100.0)
        if st["fill_pct"] >= 95.0:  # collected
            st["fill_pct"] = random.uniform(2.0, 6.0)
        st["battery_pct"] = max(0.0, st["battery_pct"] - 0.002)
        records.append({
            "bin_id": b["bin_id"],
            "fill_pct": round(st["fill_pct"], 2),
            "battery_pct": round(st["battery_pct"], 2),
            "event_ts": now_ms,
        })
    return records


def footfall_records(sim_minute):
    now_ms = int(time.time() * 1000)
    return [{"district_id": d["id"], "people_count": int(footfall(sim_minute, d["mult"])), "event_ts": now_ms, } for d
            in DISTRICTS]


def spike(district_id, factor=4.0):
    """Demo helper: dump waste into one district's bins immediately."""
    for b in BINS:
        if b["district_id"] == district_id:
            st = _state[b["bin_id"]]
            st["fill_pct"] = min(100.0, st["fill_pct"] + 12.0 * factor)
