CREATE TABLE IF NOT EXISTS transaction_report(
    id NUMERIC(12) PRIMARY KEY,
    transaction_date DATE,
    amount NUMERIC (10),
    operation_type VARCHAR(20)
)