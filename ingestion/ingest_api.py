import requests
import pandas as pd
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning) 
## I disabled SSL verification locally due to network constraints, but in production this would use proper certificate handling

def ingest_api_users() -> pd.DataFrame:
    """Fetch users from JSONPlaceholder REST API. Returns raw dataframe."""
    response = requests.get("https://jsonplaceholder.typicode.com/users", verify=False)
    response.raise_for_status()
    users = response.json()
    df = pd.DataFrame(users)
    df["source"] = "api"
    print(f"✓ API: {len(df)} records ingested")
    return df

if __name__ == "__main__":
    df = ingest_api_users()
    print(df[["id", "name", "email"]].head())