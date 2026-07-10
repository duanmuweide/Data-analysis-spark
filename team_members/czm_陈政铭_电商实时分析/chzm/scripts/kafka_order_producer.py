#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Replay generated order events into Kafka to simulate a realtime order stream.

Default input:
  ../data/raw/order_events.jsonl

Example:
  python kafka_order_producer.py --bootstrap-server master-pc:9092 --topic czm_order_events --speed 10

Dependency on the machine that runs this script:
  pip install kafka-python
"""

from __future__ import annotations

import argparse
import json
import signal
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterator, Optional


DEFAULT_INPUT = Path(__file__).resolve().parents[1] / "data" / "raw" / "order_events.jsonl"
DEFAULT_BOOTSTRAP_SERVER = "master-pc:9092"
DEFAULT_TOPIC = "czm_order_events"
REQUIRED_FIELDS = {
    "order_id",
    "user_id",
    "order_time",
    "province",
    "city",
    "category",
    "pay_amount",
    "order_status",
    "channel",
}

STOP_REQUESTED = False


def request_stop(signum: int, frame: object) -> None:
    global STOP_REQUESTED
    STOP_REQUESTED = True


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Send order_events.jsonl records to Kafka.")
    parser.add_argument("--input", default=str(DEFAULT_INPUT), help="Path to order_events.jsonl")
    parser.add_argument("--bootstrap-server", default=DEFAULT_BOOTSTRAP_SERVER, help="Kafka bootstrap server")
    parser.add_argument("--topic", default=DEFAULT_TOPIC, help="Kafka topic")
    parser.add_argument("--speed", type=float, default=10.0, help="Records per second; use 0 for unlimited")
    parser.add_argument("--max-records", type=int, default=0, help="Stop after N records; 0 means no limit")
    parser.add_argument("--loop", action="store_true", help="Replay the input file repeatedly")
    parser.add_argument("--realtime-clock", action="store_true", help="Replace order_time with current time when sending")
    parser.add_argument("--dry-run", action="store_true", help="Validate and print records without sending to Kafka")
    return parser.parse_args()


def iter_events(path: Path) -> Iterator[Dict[str, object]]:
    with path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSON at line {line_no}: {exc}") from exc

            missing = REQUIRED_FIELDS - event.keys()
            if missing:
                fields = ", ".join(sorted(missing))
                raise ValueError(f"Missing required fields at line {line_no}: {fields}")

            yield event


def load_kafka_producer_class():
    try:
        from kafka import KafkaProducer
    except ImportError as exc:
        raise RuntimeError(
            "Missing dependency kafka-python. Install it on the machine running this script: "
            "pip install kafka-python"
        ) from exc
    return KafkaProducer


def create_producer(bootstrap_server: str):
    kafka_producer = load_kafka_producer_class()
    servers = [item.strip() for item in bootstrap_server.split(",") if item.strip()]
    return kafka_producer(
        bootstrap_servers=servers,
        key_serializer=lambda value: value.encode("utf-8") if value is not None else None,
        value_serializer=lambda value: value.encode("utf-8"),
        retries=3,
        linger_ms=50,
    )


def send_events(args: argparse.Namespace) -> int:
    input_path = Path(args.input).expanduser().resolve()
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}. Run generate_ecommerce_data.py first.")

    delay_seconds = 0.0 if args.speed <= 0 else 1.0 / args.speed
    sent = 0

    if args.dry_run:
        for event in iter_events(input_path):
            print(json.dumps(event, ensure_ascii=False))
            sent += 1
            if args.max_records and sent >= args.max_records:
                break
        print(f"Dry run validated {sent} event(s).")
        return sent

    producer = create_producer(args.bootstrap_server)
    try:
        while not STOP_REQUESTED:
            for event in iter_events(input_path):
                if STOP_REQUESTED:
                    break

                if args.realtime_clock:
                    event = dict(event)
                    event["order_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                key = str(event["order_id"])
                value = json.dumps(event, ensure_ascii=False, separators=(",", ":"))
                producer.send(args.topic, key=key, value=value)
                sent += 1

                if sent % 100 == 0:
                    producer.flush()
                    print(f"Sent {sent} events to topic {args.topic}")

                if args.max_records and sent >= args.max_records:
                    producer.flush()
                    return sent

                if delay_seconds > 0:
                    time.sleep(delay_seconds)

            if not args.loop:
                break

        producer.flush()
        return sent
    finally:
        producer.close()


def main() -> None:
    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    args = parse_args()
    sent = send_events(args)
    print(f"Finished. Total events sent: {sent}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
