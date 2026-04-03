-- Dimension table: unified customer profiles
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_key        VARCHAR(100) PRIMARY KEY,
    full_name           VARCHAR(255),
    email               VARCHAR(255) UNIQUE,
    signup_date         DATE,
    country             VARCHAR(100),
    plan                VARCHAR(50),
    annual_revenue_usd  NUMERIC(12,2),
    source              VARCHAR(50),
    loaded_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Fact table: product usage events
CREATE TABLE IF NOT EXISTS fact_user_activity (
    event_id            VARCHAR(100) PRIMARY KEY,
    customer_key        VARCHAR(100),
    event_type          VARCHAR(100),
    event_date          DATE,
    session_duration_s  INTEGER,
    source              VARCHAR(50),
    loaded_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);