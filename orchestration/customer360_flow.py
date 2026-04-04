from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta

from ingestion.ingest_api import ingest_api_users
from ingestion.ingest_csv import ingest_crm_csv
from ingestion.ingest_events import ingest_usage_events
from transform.normalise import build_customer_profiles, normalise_events
from load.warehouse import get_engine, create_tables, load_profiles, load_events
from quality.checks import run_quality_checks

# --- Tasks ---

@task(name="ingest_api", retries=3, retry_delay_seconds=10)
def task_ingest_api():
    return ingest_api_users()

@task(name="ingest_csv", retries=2, retry_delay_seconds=5)
def task_ingest_csv():
    return ingest_crm_csv()

@task(name="ingest_events", retries=2, retry_delay_seconds=5)
def task_ingest_events():
    return ingest_usage_events()

@task(name="transform")
def task_transform(api_raw, csv_raw, events_raw):
    profiles = build_customer_profiles(api_raw, csv_raw)
    events = normalise_events(events_raw)
    return profiles, events

@task(name="load")
def task_load(profiles, events):
    engine = get_engine()
    create_tables(engine)
    load_profiles(profiles, engine)
    load_events(events, engine)
    return engine

@task(name="quality_checks")
def task_quality_checks(profiles, events, engine):
    run_quality_checks(profiles, events, engine)

# --- Flow ---

@flow(name="customer360-pipeline", log_prints=True)
def customer360_pipeline():
    """Daily Customer 360 pipeline — ingest, transform, load, validate."""

    # Ingest
    api_raw = task_ingest_api()
    csv_raw = task_ingest_csv()
    events_raw = task_ingest_events()

    # Transform
    profiles, events = task_transform(api_raw, csv_raw, events_raw)

    # Load
    engine = task_load(profiles, events)

    # Quality
    task_quality_checks(profiles, events, engine)

if __name__ == "__main__":
    customer360_pipeline()