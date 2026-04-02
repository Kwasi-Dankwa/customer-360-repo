## running quick diagnostic to view raw dataframe
# having an idea of columsn I'll be working with
from ingestion.ingest_api import ingest_api_users
from ingestion.ingest_csv import ingest_crm_csv
from ingestion.ingest_events import ingest_usage_events

print("=== API columns ===")
print(ingest_api_users().columns.tolist())

print("=== CSV columns ===")
print(ingest_crm_csv().columns.tolist())

print("=== Events columns ===")
print(ingest_usage_events().columns.tolist())