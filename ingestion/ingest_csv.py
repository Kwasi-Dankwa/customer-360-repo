import pandas as pd

def ingest_crm_csv(path="data/crm_customers.csv") -> pd.DataFrame:
    """Load CRM data from legacy CSV export. Returns raw dataframe."""
    df = pd.read_csv(path)
    df["source"] = "csv"
    print(f"✓ CSV: {len(df)} records ingested")
    return df

if __name__ == "__main__":
    df = ingest_crm_csv()
    print(df.head())