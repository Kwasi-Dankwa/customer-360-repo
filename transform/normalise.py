# creating a unified schema

import pandas as pd
def normalise_api(df: pd.DataFrame) -> pd.DataFrame:
    """Normalise API users into unified schema."""
    normalised = pd.DataFrame()
    normalised["customer_key"] = df["id"].astype(str)
    normalised["full_name"] = df["name"]
    normalised["email"] = df["email"]
    normalised["signup_date"] = pd.NaT          # API doesn't provide this
    normalised["country"] = df["address"].apply(
        lambda x: x.get("city", None) if isinstance(x, dict) else None
    )
    normalised["plan"] = None                    # API doesn't provide this
    normalised["annual_revenue_usd"] = None      # API doesn't provide this
    normalised["source"] = "api"
    print(f"✓ API normalised: {len(normalised)} records")
    return normalised


def normalise_csv(df: pd.DataFrame) -> pd.DataFrame:
    """Normalise CSV CRM data into unified schema."""
    normalised = pd.DataFrame()
    normalised["customer_key"] = df["id"].astype(str)
    normalised["full_name"] = df["full_name"]
    normalised["email"] = df["email"]
    normalised["signup_date"] = pd.to_datetime(
        df["signup_date"], format="%d/%m/%Y", errors="coerce"
    )
    normalised["country"] = df["country"]
    normalised["plan"] = df["plan"].fillna("free")
    normalised["annual_revenue_usd"] = df["annual_revenue_usd"]
    normalised["source"] = "csv"
    print(f"✓ CSV normalised: {len(normalised)} records")
    return normalised


def normalise_events(df: pd.DataFrame) -> pd.DataFrame:
    """Normalise usage events into unified schema."""
    normalised = pd.DataFrame()
    normalised["customer_key"] = df["customerId"].astype(str)
    normalised["event_id"] = df["event_id"]
    normalised["event_type"] = df["event_type"]
    normalised["timestamp"] = pd.to_datetime(
        df["timestamp"], utc=True, errors="coerce"
    ).dt.date
    normalised["session_duration_s"] = df["session_duration_s"]
    normalised["source"] = "events"
    print(f"✓ Events normalised: {len(normalised)} records")
    return normalised


def build_customer_profiles(api_df, csv_df) -> pd.DataFrame:
    """Merge API and CSV into a single customer profiles table."""
    api_norm = normalise_api(api_df)
    csv_norm = normalise_csv(csv_df)
    profiles = pd.concat([api_norm, csv_norm], ignore_index=True)
    profiles = profiles.drop_duplicates(subset=["email"])
    print(f"✓ Customer profiles built: {len(profiles)} records")
    return profiles