-- Insert a new customer into the core banking system
INSERT INTO client.customers (first_name, last_name, date_of_birth) 
VALUES ('Alice', 'Smith', '1985-05-15');

-- Insert a transaction into the core banking system
INSERT INTO payments.transactions (sender_account_id, receiver_account_id, amount, currency) 
VALUES (101, 202, 1500.00, 'GBP');
