CREATE SCHEMA bcdm;

-- Barclays Conceptual Data Model (BCDM) - Simplified Mockup
-- Standardizing disparate source entities into foundational business concepts

-- The 'Party' entity holds clients, organizations, employees etc.
CREATE TABLE bcdm.party (
    party_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    party_type VARCHAR(50), -- e.g., 'INDIVIDUAL', 'ORGANIZATION'
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    date_of_birth DATE,
    -- Lineage tracking
    source_system VARCHAR(50),
    source_record_id VARCHAR(50),
    integration_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- The 'Arrangement' entity holds products, accounts, and loans
CREATE TABLE bcdm.arrangement (
    arrangement_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    party_id UUID REFERENCES bcdm.party(party_id),
    product_category VARCHAR(50), -- e.g., 'LOAN', 'CHECKING_ACCOUNT'
    balance DECIMAL(15, 2),
    status VARCHAR(50),
    -- Lineage tracking
    source_system VARCHAR(50),
    source_record_id VARCHAR(50),
    integration_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- The 'Event' entity holds interactions and financial transactions
CREATE TABLE bcdm.event (
    event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type VARCHAR(50), -- e.g., 'PAYMENT', 'CUSTOMER_CALL'
    related_party_id UUID REFERENCES bcdm.party(party_id),
    related_arrangement_id UUID,
    event_amount DECIMAL(15, 2),
    currency VARCHAR(3),
    event_description TEXT,
    event_timestamp TIMESTAMP,
    -- Lineage tracking
    source_system VARCHAR(50),
    source_record_id VARCHAR(50),
    integration_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
