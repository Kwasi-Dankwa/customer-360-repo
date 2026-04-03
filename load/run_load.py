import traceback
from ingestion.ingest_api import ingest_api_users
from ingestion.ingest_csv import ingest_crm_csv
from ingestion.ingest_events import ingest_usage_events
from transform.normalise import build_customer_profiles, normalise_events
from load.warehouse import get_engine, create_tables, load_profiles, load_events

if __name__ == "__main__":
    try:
        print("Starting pipeline...")

        # Ingest
        api_raw = ingest_api_users()
        csv_raw = ingest_crm_csv()
        events_raw = ingest_usage_events()

        # Transform
        profiles = build_customer_profiles(api_raw, csv_raw)
        events = normalise_events(events_raw)

        # Load
        print("Connecting to database...")
        engine = get_engine()
        create_tables(engine)
        load_profiles(profiles, engine)
        load_events(events, engine)

        print("\n✓ Pipeline complete — data landed in PostgreSQL")

    except Exception as e:
        print(f"\n✗ Pipeline failed: {e}")
        traceback.print_exc()
