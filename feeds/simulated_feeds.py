# Simulated Feed 1-10
import random
from datetime import datetime, timedelta

FEED_WEIGHTS = {
    'feed1': 1.0,
    'feed2': 0.8,
    'feed3': 1.2,
    'feed4': 0.9,
    'feed5': 0.7,
    'feed6': 1.1,
    'feed7': 0.95,
    'feed8': 1.05,
    'feed9': 0.6,
    'feed10': 0.85,
}

FEED_NAMES = list(FEED_WEIGHTS.keys())

# Simulate each feed returning a date, sometimes conflicting

def simulate_feed_data(symbol: str):
    base_date = datetime(2025, 5, 15)
    results = {}
    for feed in FEED_NAMES:
        # Each feed may have +/- 2 days of variance
        variance = random.choice([-2, -1, 0, 1, 2])
        date = base_date + timedelta(days=variance)
        # Some feeds may return None (missing data)
        if random.random() < 0.1:
            date = None
        results[feed] = date
    return results
