import requests
import pandas as pd

def ingest_api_users() -> pd.DataFrame:
    """Fetch users from JSONPlaceholder REST API. Returns raw dataframe."""
    response = requests.get("https://jsonplaceholder.typicode.com/users")
    response.raise_for_status()
    users = response.json()
    df = pd.DataFrame(users)
    df["source"] = "api"
    print(f"✓ API: {len(df)} records ingested")
    return df

if __name__ == "__main__":
    df = ingest_api_users()
    print(df[["id", "name", "email"]].head())