CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.payments_transactions (
    txn_id INT PRIMARY KEY,
    sender_account_id INT,
    receiver_account_id INT,
    amount DECIMAL,
    currency VARCHAR(3),
    txn_time TIMESTAMP,
    landed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Approach 3: Virtualize the transformation using a Database View
-- Downstream systems query this view exactly as if it were the physical bcdm.event table.
CREATE OR REPLACE VIEW bcdm.virtual_event AS 
SELECT 
    -- Compute the UUID on the fly during the query
    md5(txn_id::text)::uuid AS event_id, 
    'PAYMENT_TRANSACTION'::VARCHAR(50) AS event_type,
    NULL::uuid AS related_party_id,
    NULL::uuid AS related_arrangement_id,
    amount AS event_amount,
    currency AS currency,
    NULL::text AS event_description,
    txn_time AS event_timestamp,
    'CORE_BANKING_PAYMENTS'::VARCHAR(50) AS source_system,
    txn_id::text AS source_record_id,
    landed_at AS integration_timestamp
FROM raw.payments_transactions;
