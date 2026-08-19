CREATE TABLE IF NOT EXISTS transaction(
    id NUMERIC(12) PRIMARY KEY,
    transaction_date DATE,
    amount NUMERIC (10, 2)
    operation_type VARCHAR(20)
)