import sys
import json
from unittest.mock import patch, MagicMock

# We must mock psycopg2 and confluent_kafka before importing the transformer
# because transformer.py executes database connection logic at the module level.
sys.modules['confluent_kafka'] = MagicMock()

patcher = patch('psycopg2.connect')
mock_connect = patcher.start()

# Now we can safely import our module
import transformer

def test_process_client_customer():
    payload = {
        'customer_id': 123,
        'first_name': 'Alice',
        'last_name': 'Smith'
    }
    
    with patch.object(transformer, 'cursor') as mock_cursor:
        transformer.process_client_customer(payload)
        
        # Verify cursor.execute was called once
        assert mock_cursor.execute.call_count == 1
        
        # Get the arguments it was called with
        args, _ = mock_cursor.execute.call_args
        sql = args[0]
        params = args[1]
        
        assert "INSERT INTO bcdm.party" in sql
        assert params[1] == 'INDIVIDUAL'
        assert params[2] == 'Alice'
        assert params[3] == 'Smith'
        assert params[4] == 'CORE_BANKING_CLIENT'
        assert params[5] == '123'

def test_process_payments_transaction():
    payload = {
        'txn_id': 456,
        'amount': '250.50',
        'currency': 'EUR'
    }
    
    with patch.object(transformer, 'cursor') as mock_cursor:
        transformer.process_payments_transaction(payload)
        
        assert mock_cursor.execute.call_count == 1
        args, _ = mock_cursor.execute.call_args
        sql = args[0]
        params = args[1]
        
        assert "INSERT INTO bcdm.event" in sql
        assert params[1] == 'PAYMENT_TRANSACTION'
        assert params[2] == '250.50'
        assert params[3] == 'EUR'
        assert params[4] == 'CORE_BANKING_PAYMENTS'
        assert params[5] == '456'

def test_process_message_valid_client_creation():
    msg = MagicMock()
    msg.topic.return_value = 'src.client.customers'
    msg.value.return_value = b'{"payload": {"op": "c", "after": {"customer_id": 999, "first_name": "Bob"}}}'
    
    with patch.object(transformer, 'process_client_customer') as mock_process:
        transformer.process_message(msg)
        mock_process.assert_called_once_with({"customer_id": 999, "first_name": "Bob"})

def test_process_message_ignores_delete_operations():
    msg = MagicMock()
    msg.topic.return_value = 'src.client.customers'
    msg.value.return_value = b'{"payload": {"op": "d", "before": {"customer_id": 999}}}'
    
    with patch.object(transformer, 'process_client_customer') as mock_process:
        transformer.process_message(msg)
        # We ignore deletes in our PoC, so it should not process
        mock_process.assert_not_called()

def test_process_message_handles_malformed_json():
    msg = MagicMock()
    msg.value.return_value = b'{"invalid_json": }'
    
    with patch.object(transformer, 'logger') as mock_logger:
        transformer.process_message(msg)
        # Should catch the JSON decode error and log it
        assert mock_logger.error.call_count == 1
