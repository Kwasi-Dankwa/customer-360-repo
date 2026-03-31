from faker import Faker
import pandas as pd
import json
import random
import os

fake = Faker()

def generate_crm_csv(n=200, path="data/crm_customers.csv"):
    rows = []
    for _ in range(n):
        rows.append({
            "id": fake.uuid4(),
            "full_name": fake.name(),
            "email": fake.email(),
            "signup_date": fake.date_between("-2y", "today").strftime("%d/%m/%Y"),
            "country": fake.country_code(),
            "plan": random.choice(["free", "pro", "enterprise", None]),
            "annual_revenue_usd": round(random.uniform(0, 50000), 2),
        })
    pd.DataFrame(rows).to_csv(path, index=False)
    print(f"✓ CRM CSV written → {path}")

def generate_events_json(n=500, path="data/usage_events.json"):
    event_types = ["page_view", "feature_click", "export", "login", "upgrade_prompt"]
    events = []
    for _ in range(n):
        ts = fake.date_time_between("-90d", "now")
        events.append({
            "event_id": fake.uuid4(),
            "customerId": fake.uuid4(),
            "event_type": random.choice(event_types),
            "timestamp": ts.isoformat() + "Z",
            "session_duration_s": random.randint(10, 3600),
            "properties": {
                "page": fake.uri_path(),
                "browser": random.choice(["Chrome", "Firefox", "Safari", None]),
            }
        })
    with open(path, "w") as f:
        json.dump(events, f, indent=2)
    print(f"✓ Events JSON written → {path}")

if __name__ == "__main__":
    os.makedirs("data", exist_ok=True)
    generate_crm_csv()
    generate_events_json()