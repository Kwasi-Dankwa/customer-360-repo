import pandas as pd
from sqlalchemy import create_engine, text
from load.warehouse import get_engine
from datetime import datetime

def log_result(engine, check_name, table_name, status, expected, actual, notes=""):
    """Write a single quality check result to pipeline_runs."""
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO pipeline_runs 
                (check_name, table_name, status, expected_value, actual_value, notes)
            VALUES 
                (:check_name, :table_name, :status, :expected, :actual, :notes)
        """), {
            "check_name": check_name,
            "table_name": table_name,
            "status": status,
            "expected": expected,
            "actual": actual,
            "notes": notes
        })
        conn.commit()

def check_row_counts(engine, profiles_df, events_df):
    """Check that row counts meet minimum thresholds."""
    checks = [
        ("row_count", "dim_customers", len(profiles_df), 200),
        ("row_count", "fact_user_activity", len(events_df), 400),
    ]
    for check_name, table, actual, minimum in checks:
        status = "PASS" if actual >= minimum else "FAIL"
        notes = f"Minimum threshold: {minimum}"
        log_result(engine, check_name, table, status, minimum, actual, notes)
        print(f"  {'✓' if status == 'PASS' else '✗'} {table} row count: {actual} ({status})")

def check_null_percentage(engine, df, table_name, critical_columns, threshold=0.2):
    """Fail if null % in any critical column exceeds threshold."""
    for col in critical_columns:
        if col not in df.columns:
            continue
        null_pct = df[col].isnull().mean()
        status = "PASS" if null_pct <= threshold else "FAIL"
        notes = f"Null threshold: {threshold*100}%"
        log_result(engine, "null_check", table_name, status, threshold, float(round(null_pct, 4)), notes)
        print(f"  {'✓' if status == 'PASS' else '✗'} {table_name}.{col} null%: {round(null_pct*100,1)}% ({status})")

def run_quality_checks(profiles_df, events_df, engine):
    """Run all quality checks and log results."""
    print("\n=== Running quality checks ===")

    check_row_counts(engine, profiles_df, events_df)

    check_null_percentage(
        engine, profiles_df, "dim_customers",
        critical_columns=["customer_key", "email", "source"]
    )
    check_null_percentage(
        engine, events_df, "fact_user_activity",
        critical_columns=["customer_key", "event_type", "event_date"]
    )

    print("\n=== Quality checks complete — results logged to pipeline_runs ===")
    