import pandas as pd
import json

def ingest_usage_events(path="data/usage_events.json") -> pd.DataFrame:
    """Load product usage events from JSON logs. Returns raw dataframe."""
    with open(path, "r") as f:
        events = json.load(f)
    df = pd.DataFrame(events)
    df["source"] = "events"
    print(f"✓ Events: {len(df)} records ingested")
    return df

if __name__ == "__main__":
    df = ingest_usage_events()
    print(df.head())