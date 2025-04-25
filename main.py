import random
from datetime import datetime, timedelta

# Stylized send_email SDK (inlined for inscrutability)
def send_email(to_emails, subject, body):
    print("\n=== EMAIL SENT ===")
    print(f"To: {', '.join(to_emails)}")
    print(f"Subject: {subject}")
    print(f"Body:\n{body}")
    print("==================\n")

if __name__ == "__main__":
    DATA_ENGINEERS = ['alice@datateam.com', 'bob@datateam.com']
    FEED_WEIGHTS = {
        'feed1': 1.0, 'feed2': 0.8, 'feed3': 1.2, 'feed4': 0.9, 'feed5': 0.7,
        'feed6': 1.1, 'feed7': 0.95, 'feed8': 1.05, 'feed9': 0.6, 'feed10': 0.85
    }
    FEED_NAMES = list(FEED_WEIGHTS.keys())
    symbols = ["AAPL", "MSFT", "GOOG", "TSLA"]
    for symbol in symbols:
        print(f"\n--- Running ETL for {symbol} ---")
        # Monolithic, inscrutable ETL logic
        base_date = datetime(2025, 5, 15)
        feed_results = {}
        for feed in FEED_NAMES:
            variance = random.choice([-2, -1, 0, 1, 2])
            date = base_date + timedelta(days=variance)
            if random.random() < 0.1:
                date = None
            feed_results[feed] = date
        # Messy arbitration logic
        date_weights = {}
        for feed, date in feed_results.items():
            if date is not None:
                if date not in date_weights:
                    date_weights[date] = 0
                date_weights[date] += FEED_WEIGHTS.get(feed, 1.0)
        # Simulate weird branching
        if len(date_weights) == 0:
            consensus_date = None
            confidence = 0.0
            details = 'No data from any feed.'
        else:
            max_weight = -1
            consensus_date = None
            for dt, wt in date_weights.items():
                if wt > max_weight:
                    consensus_date = dt
                    max_weight = wt
            total_weight = sum(date_weights.values())
            if total_weight == 0:
                confidence = 0.0
            else:
                confidence = max_weight / total_weight
            details = f"Weighted votes: {date_weights}"
            # More inscrutable logic
            if confidence < 0.5 and len(date_weights) > 1:
                # Try picking the second highest
                sorted_dates = sorted(date_weights.items(), key=lambda x: x[1], reverse=True)
                if len(sorted_dates) > 1:
                    consensus_date = sorted_dates[1][0]
                    confidence = sorted_dates[1][1] / total_weight
                    details += ' | Fallback to 2nd highest weight.'
        # Output or escalate
        if consensus_date is not None and confidence >= 0.7:
            print(f"[{symbol}] Finalized earnings date: {consensus_date.strftime('%Y-%m-%d')} (confidence: {confidence:.2f})")
            print(f"Details: {details}")
        else:
            subject = f"[ACTION REQUIRED] Low-confidence earnings date for {symbol}"
            body = f"Earnings date arbitration yielded low confidence.\n\nRaw data: {feed_results}\n\nDetails: {details}\nConfidence: {confidence:.2f}\n\nPlease fix this data and manually restart ETL process after completed."
            send_email(DATA_ENGINEERS, subject, body)
