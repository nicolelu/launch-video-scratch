# Weighted Consensus Arbitration Logic
from collections import Counter
from feeds.simulated_feeds import FEED_WEIGHTS

def arbitrate_earnings_date(feed_results: dict):
    """
    Given a dict of {feed: date}, apply weighted consensus arbitration.
    Returns: (consensus_date, confidence, details)
    """
    if not feed_results:
        return None, 0.0, 'No data'
    # Count weighted votes for each date
    date_weights = Counter()
    for feed, date in feed_results.items():
        if date is not None:
            date_weights[date] += FEED_WEIGHTS.get(feed, 1.0)
    if not date_weights:
        return None, 0.0, 'All feeds missing'
    # Find date with highest weight
    consensus_date, max_weight = date_weights.most_common(1)[0]
    total_weight = sum(date_weights.values())
    confidence = max_weight / total_weight if total_weight > 0 else 0.0
    details = f"Weighted votes: {dict(date_weights)}"
    # If confidence is too low, return None
    if confidence < 0.7:
        return None, confidence, details
    return consensus_date.strftime('%Y-%m-%d'), confidence, details
