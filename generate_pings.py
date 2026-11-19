#!/usr/bin/env python
"""Generate deterministic mock mobile pings and store locations for Hyderabad."""

import argparse
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path


CITY_BOUNDS = (17.30, 17.50, 78.30, 78.55)
START_UTC = datetime(2025, 1, 1)
HOUR_WEIGHTS = [
    0.2, 0.1, 0.1, 0.1, 0.1, 0.2, 0.4, 1.8, 2.2, 1.8, 0.8, 0.7,
    0.7, 0.7, 0.7, 0.8, 1.0, 2.0, 2.4, 1.8, 0.7, 0.5, 0.4, 0.3,
]
STORE_NAMES = [
    "GeoPulse Central", "Banjara Hills Market", "Jubilee Hills Shop",
    "Hitech City Outlet", "Gachibowli Store", "Madhapur Market",
    "Kukatpally Shop", "Secunderabad Store", "Begumpet Market",
    "Ameerpet Outlet", "Kondapur Store", "Manikonda Market",
    "Miyapur Shop", "Uppal Store", "LB Nagar Market",
    "Mehdipatnam Outlet", "Nallagandla Store", "Tarnaka Market",
    "Kompally Shop", "Financial District Store",
]


def weighted_hour(rng, cumulative_weights, total_weight):
    value = rng.random() * total_weight
    low, high = 0, len(cumulative_weights)
    while low < high:
        middle = (low + high) // 2
        if value < cumulative_weights[middle]:
            high = middle
        else:
            low = middle + 1
    return low


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output-dir", type=Path, default=Path("data"))
    args = parser.parse_args()
    if args.rows < 1:
        parser.error("--rows must be a positive integer")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    pings_path = args.output_dir / "pings.csv"
    stores_path = args.output_dir / "stores.csv"
    rng = random.Random(args.seed)
    south, north, west, east = CITY_BOUNDS

    # Keep stores and device home/work points within city bounds.
    with stores_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(("store_id", "name", "latitude", "longitude"))
        for index, name in enumerate(STORE_NAMES, start=1):
            writer.writerow(
                (
                    f"store_{index:03d}",
                    name,
                    f"{rng.uniform(south, north):.6f}",
                    f"{rng.uniform(west, east):.6f}",
                )
            )

    device_count = min(200_000, max(1_000, args.rows // 50))
    homes = [
        (rng.uniform(south, north), rng.uniform(west, east))
        for _ in range(device_count)
    ]
    workplaces = [
        (rng.uniform(south, north), rng.uniform(west, east))
        for _ in range(device_count)
    ]
    weights = HOUR_WEIGHTS * 3
    cumulative_weights = []
    total_weight = 0.0
    for weight in weights:
        total_weight += weight
        cumulative_weights.append(total_weight)

    # Stream output so larger runs do not need to fit in memory.
    with pings_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(("device_id", "latitude", "longitude", "ping_timestamp"))
        for row_number in range(1, args.rows + 1):
            device_index = rng.randrange(device_count)
            hour = weighted_hour(rng, cumulative_weights, total_weight)
            elapsed_seconds = hour * 3600 + rng.randrange(3600)
            hour_of_day = (elapsed_seconds % 86400) / 3600
            home = homes[device_index]
            work = workplaces[device_index]

            if 7 <= hour_of_day < 10:
                progress = (hour_of_day - 7) / 3
                latitude = home[0] + progress * (work[0] - home[0])
                longitude = home[1] + progress * (work[1] - home[1])
            elif 17 <= hour_of_day < 20:
                progress = (hour_of_day - 17) / 3
                latitude = work[0] + progress * (home[0] - work[0])
                longitude = work[1] + progress * (home[1] - work[1])
            elif 10 <= hour_of_day < 17:
                latitude, longitude = work
            else:
                latitude, longitude = home

            latitude = min(north, max(south, latitude + rng.gauss(0, 0.00035)))
            longitude = min(east, max(west, longitude + rng.gauss(0, 0.00035)))
            timestamp = START_UTC + timedelta(seconds=elapsed_seconds)
            writer.writerow(
                (
                    f"device_{device_index:08d}",
                    f"{latitude:.6f}",
                    f"{longitude:.6f}",
                    timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                )
            )
            if row_number % 100_000 == 0 or row_number == args.rows:
                print(f"Wrote {row_number:,} / {args.rows:,} pings")

    print(f"Saved {pings_path} and {stores_path}")


if __name__ == "__main__":
    main()
