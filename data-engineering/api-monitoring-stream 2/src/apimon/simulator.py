"""Simulated request logs from a fleet of microservices, with injected incidents.

Normal traffic has realistic per-service latency and a small background error
rate. Incidents (the ground truth the pipeline should catch) are written to
incidents.json. A small share of events arrive late or twice, as in real
log pipelines.

    # Write 30 simulated minutes to files (for the local streaming mode and tests)
    python -m apimon.simulator files --out data/events --minutes 30
    # Stream to Kafka in real time
    python -m apimon.simulator kafka --bootstrap localhost:9092 --topic api-requests --rate 500
"""
import argparse
import json
import math
import os
import random
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone

SERVICES = {   # service: (median latency ms, endpoints)
    "auth": (40, ["/login", "/token", "/logout"]),
    "catalog": (80, ["/products", "/products/{id}", "/search"]),
    "cart": (60, ["/cart", "/cart/items"]),
    "checkout": (150, ["/checkout", "/checkout/confirm"]),
    "payments": (220, ["/charge", "/refund"]),
}
REGIONS = ["us-east", "us-west", "eu-west"]
BASE_ERROR_RATE = 0.005


@dataclass
class Incident:
    kind: str            # latency_spike | error_burst | region_outage
    start: str           # ISO time
    end: str
    service: str | None  # None for region-wide incidents
    region: str | None


def default_incidents(t0: datetime, minutes: int) -> list[Incident]:
    """Three incidents spread across the run, each 4 minutes long."""
    def at(frac):
        return t0 + timedelta(minutes=int(minutes * frac))
    iso = lambda d: d.isoformat()   # noqa: E731
    return [
        Incident("latency_spike", iso(at(0.25)), iso(at(0.25) + timedelta(minutes=4)), "payments", None),
        Incident("error_burst", iso(at(0.5)), iso(at(0.5) + timedelta(minutes=4)), "checkout", None),
        Incident("region_outage", iso(at(0.75)), iso(at(0.75) + timedelta(minutes=4)), None, "eu-west"),
    ]


def make_event(rng: random.Random, ts: datetime, incidents: list[Incident]) -> dict:
    service = rng.choice(list(SERVICES))
    median, endpoints = SERVICES[service]
    region = rng.choice(REGIONS)
    latency = rng.lognormvariate(math.log(median), 0.35)
    error_p = BASE_ERROR_RATE
    for inc in incidents:
        if not (inc.start <= ts.isoformat() < inc.end):
            continue
        if inc.kind == "latency_spike" and service == inc.service:
            latency *= 6
        elif inc.kind == "error_burst" and service == inc.service:
            error_p = 0.35
        elif inc.kind == "region_outage" and region == inc.region:
            error_p = 0.9
    status = rng.choice([500, 502, 503]) if rng.random() < error_p else rng.choice([200] * 9 + [201, 404])
    return {"event_id": str(uuid.UUID(int=rng.getrandbits(128))), "event_time": ts.isoformat(timespec="milliseconds"),
            "service": service, "endpoint": rng.choice(endpoints), "region": region,
            "status": status, "latency_ms": round(latency, 1)}


def generate(minutes: int, rate: int, seed: int = 1, t0: datetime | None = None,
             late_share: float = 0.01, dup_share: float = 0.01):
    """Yield (batch_minute, events) for each simulated minute."""
    rng = random.Random(seed)
    t0 = t0 or datetime(2026, 11, 6, 12, 0, tzinfo=timezone.utc)
    incidents = default_incidents(t0, minutes)
    held_back = []   # late events, released in the following minute
    for m in range(minutes):
        start = t0 + timedelta(minutes=m)
        events = [make_event(rng, start + timedelta(seconds=rng.random() * 60), incidents) for _ in range(rate * 60)]
        out = held_back
        held_back = []
        for e in events:
            if rng.random() < late_share:
                held_back.append(e)            # arrives up to a minute late
            else:
                out.append(e)
            if rng.random() < dup_share:
                out.append(dict(e))            # delivered twice
        yield m, out, incidents


def to_files(out_dir: str, minutes: int, rate: int, seed: int = 1) -> list[Incident]:
    os.makedirs(out_dir, exist_ok=True)
    incidents = []
    for m, events, incidents in generate(minutes, rate, seed):
        tmp = os.path.join(out_dir, f".batch-{m:05d}.json")
        with open(tmp, "w") as f:
            f.writelines(json.dumps(e) + "\n" for e in events)
        os.rename(tmp, os.path.join(out_dir, f"batch-{m:05d}.json"))   # atomic: Spark never sees half a file
    json.dump([asdict(i) for i in incidents], open(os.path.join(out_dir, "..", "incidents.json"), "w"), indent=1)
    return incidents


def to_kafka(bootstrap: str, topic: str, rate: int, minutes: int, seed: int = 1):
    from kafka import KafkaProducer

    producer = KafkaProducer(bootstrap_servers=bootstrap, value_serializer=lambda v: json.dumps(v).encode())
    t0 = datetime.now(timezone.utc)
    incidents = default_incidents(t0, minutes)
    json.dump([asdict(i) for i in incidents], open("incidents.json", "w"), indent=1)
    rng = random.Random(seed)
    for _ in range(minutes * 60):
        tick = time.monotonic()
        now = datetime.now(timezone.utc)
        for _ in range(rate):
            e = make_event(rng, now, incidents)
            producer.send(topic, e)
            if rng.random() < 0.01:
                producer.send(topic, e)                       # duplicate delivery
        producer.flush()
        time.sleep(max(0.0, 1 - (time.monotonic() - tick)))


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="mode", required=True)
    f = sub.add_parser("files")
    f.add_argument("--out", default="data/events")
    f.add_argument("--minutes", type=int, default=30)
    f.add_argument("--rate", type=int, default=20, help="events per second")
    k = sub.add_parser("kafka")
    k.add_argument("--bootstrap", default="localhost:9092")
    k.add_argument("--topic", default="api-requests")
    k.add_argument("--rate", type=int, default=200)
    k.add_argument("--minutes", type=int, default=30)
    args = p.parse_args(argv)
    if args.mode == "files":
        incs = to_files(args.out, args.minutes, args.rate)
        print(f"wrote {args.minutes} minutes to {args.out}; incidents: {[i.kind for i in incs]}")
    else:
        to_kafka(args.bootstrap, args.topic, args.rate, args.minutes)


if __name__ == "__main__":
    main()
