import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()

def get_engine():
    """Create SQLAlchemy engine from .env credentials."""
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    url = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    return create_engine(url)

def create_tables(engine):
    """Run SQL schema file to create tables if they don't exist."""
    with open("sql/create_tables.sql", "r") as f:
        sql = f.read()
    with engine.connect() as conn:
        conn.execute(text(sql))
        conn.commit()
    print("✓ Tables created (or already exist)")

def load_profiles(df: pd.DataFrame, engine):
    """Load customer profiles into dim_customers."""
    df.to_sql(
        "dim_customers",
        engine,
        if_exists="append",
        index=False,
        method="multi"
    )
    print(f"✓ Loaded {len(df)} records into dim_customers")

def load_events(df: pd.DataFrame, engine):
    """Load usage events into fact_user_activity."""
    df_renamed = df.rename(columns={"timestamp": "event_date"})
    df_renamed.to_sql(
        "fact_user_activity",
        engine,
        if_exists="append",
        index=False,
        method="multi"
    )
    print(f"✓ Loaded {len(df_renamed)} records into fact_user_activity")