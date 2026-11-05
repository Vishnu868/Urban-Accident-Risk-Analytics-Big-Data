"""Publishes simulated live traffic + weather events to Kafka (topic: traffic_events)."""
import argparse
import json
import random
import time
from datetime import datetime

from kafka import KafkaProducer

# A few city zones (lat, lng) - replace with your city's grid
ZONES = {
    "Gandhipuram": (11.0168, 76.9558), "Peelamedu": (11.0270, 77.0290),
    "Ukkadam": (10.9900, 76.9610), "Saibaba Colony": (11.0230, 76.9400),
    "Singanallur": (10.9990, 77.0320), "RS Puram": (11.0080, 76.9500),
}
WEATHER = ["Clear", "Clouds", "Rain", "Heavy Rain", "Fog"]


def make_event():
    zone, (lat, lng) = random.choice(list(ZONES.items()))
    hour = datetime.now().hour
    rush = hour in (8, 9, 10, 17, 18, 19)
    weather = random.choices(WEATHER, weights=[50, 25, 15, 5, 5])[0]
    return {
        "event_time": datetime.now().isoformat(timespec="seconds"),
        "zone": zone,
        "lat": lat + random.uniform(-0.005, 0.005),
        "lng": lng + random.uniform(-0.005, 0.005),
        "hour": hour,
        "day_of_week": datetime.now().isoweekday(),
        "avg_speed_kmph": max(3.0, random.gauss(18 if rush else 38, 8)),
        "vehicle_count": int(random.gauss(220 if rush else 90, 30)),
        "temperature_c": random.gauss(29, 3),
        "humidity": random.uniform(40, 95),
        "visibility_km": random.uniform(0.3, 2) if weather == "Fog" else random.uniform(4, 10),
        "precipitation_mm": random.uniform(5, 30) if "Rain" in weather else 0.0,
        "weather": weather,
        "social_incident_reports": random.choices([0, 1, 2, 3], weights=[85, 10, 4, 1])[0],
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="traffic_events")
    ap.add_argument("--rate", type=float, default=20, help="events per second")
    args = ap.parse_args()

    producer = KafkaProducer(bootstrap_servers=args.bootstrap,
                             value_serializer=lambda v: json.dumps(v).encode())
    print(f"Publishing to {args.topic} @ {args.rate}/s (Ctrl+C to stop)")
    while True:
        producer.send(args.topic, make_event())
        time.sleep(1 / args.rate)


if __name__ == "__main__":
    main()
