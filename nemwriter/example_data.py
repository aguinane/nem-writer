import random
from datetime import datetime, timedelta
from random import randint, randrange

import polars as pl


def estimate_export(
    month: int, hour: int, interval: int, max_value: int = 5000
) -> float:
    """Estimate export kWh (B1) for a given hour of the day"""
    conv = interval / 60
    max_mth_val = max_value if 3 <= month <= 5 or 9 <= month <= 11 else max_value * 0.8
    if 10 <= hour < 14:
        return randint(0, int(max_mth_val)) / 1000 * conv
    elif 8 <= hour < 16:
        return randint(0, int(max_mth_val / 2)) / 1000 * conv
    elif 6 <= hour < 18:
        return randint(0, int(max_mth_val / 3)) / 1000 * conv
    return 0.0


def estimate_import(
    month: int, hour: int, interval: int, max_value: int = 8000
) -> float:
    """Estimate import kWh (E1) for a given hour of the day"""
    conv = interval / 60
    max_mth_morning = max_value * 0.8 if 6 <= month <= 9 else max_value * 0.5
    max_mth_evening = max_value if month in (1, 2, 11, 12) else max_value * 0.7
    max_mth_shoulder = max_value * 0.3
    max_mth_offpeak = max_value * 0.2
    if 16 <= hour < 20:
        return randint(0, int(max_mth_evening)) / 1000 * conv
    elif 6 <= hour < 9:
        return randint(0, int(max_mth_morning)) / 1000 * conv
    elif 20 <= hour < 22 or 11 <= hour < 14:
        return randint(0, int(max_mth_shoulder)) / 1000 * conv
    return randint(0, int(max_mth_offpeak)) / 1000 * conv


def estimate_controlled(
    month: int, hour: int, interval: int, max_value: int = 3000
) -> float:
    """Estimate controlled load kWh (E2) for a given hour of the day"""
    conv = interval / 60
    max_mth_val = max_value if month in (1, 2, 11, 12) else max_value * 0.7
    if 11 <= hour < 14:
        return randint(0, int(max_mth_val)) / 1000 * conv
    return 0.0


RANDOM_CODES = [("A", 79, "Power Outage Alarm ")]


def random_quality() -> tuple[str, int | None, str]:
    """Return a random quality, code and and reason"""
    # Return A for 99 out of 1000 records
    normal = randrange(1000) < 999
    if normal:
        return ("A", None, "")

    return random.choice(RANDOM_CODES)


def random_energy_estimate(
    month: int, hour: int, interval: int
) -> tuple[float, float, float, str, int | None, str]:
    """Estimate energy for a given hour of the day"""
    quality, code, reason = random_quality()

    e1 = round(estimate_import(month, hour, interval), 3)
    e2 = round(estimate_controlled(month, hour, interval), 3)
    b1 = round(estimate_export(month, hour, interval), 3)

    if code == 79:
        e1 = 0.0
        e2 = 0.0
        b1 = 0.0

    return e1, e2, b1, quality, code, reason


def random_energy_dataframe(
    start_time: datetime, num_days: int = 1, interval: int = 5
) -> pl.DataFrame:
    """Generate a random energy dataframe for a given number of days"""
    num_intervals = (num_days * 24 * 60) // interval
    # As we want to show the end time, we bump one interval first
    first_interval = start_time + timedelta(minutes=interval)
    timestamps = [
        first_interval + timedelta(minutes=interval * x) for x in range(num_intervals)
    ]
    data = []
    for ts in timestamps:
        e1, e2, b1, quality, code, reason = random_energy_estimate(
            ts.month, ts.hour, interval
        )
        data.append((ts, e1, e2, b1, quality, code, reason))

    df = pl.DataFrame(
        {
            "t_end": [row[0] for row in data],
            "E1": [row[1] for row in data],
            "E2": [row[2] for row in data],
            "B1": [row[3] for row in data],
            "quality": [row[4] for row in data],
            "evt_code": [row[5] for row in data],
            "evt_desc": [row[6] for row in data],
        }
    )
    return df
