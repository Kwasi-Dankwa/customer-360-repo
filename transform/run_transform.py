# testing data end to end
from ingestion.ingest_api import ingest_api_users
from ingestion.ingest_csv import ingest_crm_csv
from ingestion.ingest_events import ingest_usage_events
from transform.normalise import build_customer_profiles, normalise_events

if __name__ == "__main__":
    # Ingest raw
    api_raw = ingest_api_users()
    csv_raw = ingest_crm_csv()
    events_raw = ingest_usage_events()

    # Transform
    profiles = build_customer_profiles(api_raw, csv_raw)
    events = normalise_events(events_raw)

    # Inspect schema
    print("\n=== Customer profiles sample ===")
    print(profiles[["customer_key", "full_name", "email", "plan", "source"]].head())

    print("\n=== Events sample ===")
    print(events[["customer_key", "event_type", "timestamp"]].head())

    print("\n=== Null check: profiles ===")
    print(profiles.isnull().sum())