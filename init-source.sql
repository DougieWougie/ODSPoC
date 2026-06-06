CREATE SCHEMA client;
CREATE SCHEMA crm;
CREATE SCHEMA payments;
CREATE SCHEMA lending;

-- 1. Client Domain (Source)
CREATE TABLE client.customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    date_of_birth DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. CRM Domain (Source)
CREATE TABLE crm.interactions (
    interaction_id SERIAL PRIMARY KEY,
    client_ref_id INT,
    interaction_type VARCHAR(50),
    notes TEXT,
    interaction_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. Payments Domain (Source)
CREATE TABLE payments.transactions (
    txn_id SERIAL PRIMARY KEY,
    sender_account_id INT,
    receiver_account_id INT,
    amount DECIMAL(15, 2),
    currency VARCHAR(3),
    txn_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 4. Lending Domain (Source)
CREATE TABLE lending.loans (
    loan_id SERIAL PRIMARY KEY,
    client_ref_id INT,
    principal_amount DECIMAL(15, 2),
    interest_rate DECIMAL(5, 2),
    status VARCHAR(50),
    issued_date DATE
);

-- We need to ensure Debezium can replicate these tables
ALTER TABLE client.customers REPLICA IDENTITY FULL;
ALTER TABLE crm.interactions REPLICA IDENTITY FULL;
ALTER TABLE payments.transactions REPLICA IDENTITY FULL;
ALTER TABLE lending.loans REPLICA IDENTITY FULL;
