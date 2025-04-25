# ETL Logic
from feeds.simulated_feeds import simulate_feed_data, FEED_NAMES
from arbitration.weighted_consensus import arbitrate_earnings_date
from notifications.send_email import send_email

DATA_ENGINEERS = ['alice@datateam.com', 'bob@datateam.com']


def extract(symbol: str):
    """Extract data from all feeds for a given symbol."""
    return simulate_feed_data(symbol)


def transform(raw_data: dict):
    """Standardize data, filter out None values."""
    return {k: v for k, v in raw_data.items() if v is not None}


def load(symbol: str, consensus_date, confidence, details):
    """Load the result (here, just print or log)."""
    print(f"[{symbol}] Finalized earnings date: {consensus_date} (confidence: {confidence:.2f})")
    print(f"Details: {details}")


def escalate(symbol: str, raw_data):
    subject = f"[ACTION REQUIRED] Low-confidence earnings date for {symbol}"
    body = f"Earnings date arbitration yielded low confidence.\n\nRaw data: {raw_data}"
    send_email(DATA_ENGINEERS, subject, body)


def etl_pipeline(symbol: str):
    raw = extract(symbol)
    clean = transform(raw)
    consensus_date, confidence, details = arbitrate_earnings_date(clean)
    if consensus_date is not None and confidence >= 0.7:
        load(symbol, consensus_date, confidence, details)
    else:
        escalate(symbol, raw)
