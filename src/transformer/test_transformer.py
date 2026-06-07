import sys
import json
from unittest.mock import patch, MagicMock
import pytest

# We must mock psycopg2 and confluent_kafka before importing the transformer
# because transformer.py executes database connection logic at the module level.
sys.modules['confluent_kafka'] = MagicMock()

patcher = patch('psycopg2.connect')
mock_connect = patcher.start()

# Now we can safely import our module
import transformer

@pytest.fixture(autouse=True)
def reset_mocks():
    transformer.ods_conn.reset_mock()
    transformer.cursor.reset_mock()

def test_process_batch_with_mixed_messages():
    msg1 = MagicMock()
    msg1.topic.return_value = 'src.client.customers'
    msg1.value.return_value = b'{"payload": {"op": "c", "after": {"customer_id": 123, "first_name": "Alice", "last_name": "Smith"}}}'
    msg1.error.return_value = False
    
    msg2 = MagicMock()
    msg2.topic.return_value = 'src.payments.transactions'
    msg2.value.return_value = b'{"payload": {"op": "c", "after": {"txn_id": 456, "amount": "250.50", "currency": "EUR"}}}'
    msg2.error.return_value = False

    messages = [msg1, msg2]

    with patch('psycopg2.extras.execute_values') as mock_execute_values:
        transformer.process_batch(messages)
        
        # Verify execute_values was called twice (once for each table)
        assert mock_execute_values.call_count == 2
        
        # Check first call (customers)
        args1, _ = mock_execute_values.call_args_list[0]
        assert "INSERT INTO bcdm.party" in args1[1]
        assert args1[2][0][1] == 'INDIVIDUAL'
        assert args1[2][0][2] == 'Alice'
        assert args1[2][0][5] == '123'
        
        # Check second call (transactions)
        args2, _ = mock_execute_values.call_args_list[1]
        assert "INSERT INTO bcdm.event" in args2[1]
        assert args2[2][0][1] == 'PAYMENT_TRANSACTION'
        assert args2[2][0][2] == '250.50'
        assert args2[2][0][5] == '456'

        # Verify commit was called
        assert transformer.ods_conn.commit.call_count == 1

def test_process_batch_ignores_delete_operations():
    msg = MagicMock()
    msg.topic.return_value = 'src.client.customers'
    msg.value.return_value = b'{"payload": {"op": "d", "before": {"customer_id": 999}}}'
    msg.error.return_value = False
    
    with patch('psycopg2.extras.execute_values') as mock_execute_values:
        transformer.process_batch([msg])
        mock_execute_values.assert_not_called()
        transformer.ods_conn.commit.assert_not_called()

def test_process_batch_handles_malformed_json():
    msg = MagicMock()
    msg.value.return_value = b'{"invalid_json": }'
    msg.error.return_value = False
    
    with patch.object(transformer, 'logger') as mock_logger:
        transformer.process_batch([msg])
        # Should catch the JSON decode error and log it
        assert mock_logger.error.call_count == 1

def test_process_batch_handles_db_error():
    msg = MagicMock()
    msg.topic.return_value = 'src.client.customers'
    msg.value.return_value = b'{"payload": {"op": "c", "after": {"customer_id": 123, "first_name": "Alice"}}}'
    msg.error.return_value = False
    
    with patch('psycopg2.extras.execute_values', side_effect=Exception("DB Error")):
        with patch.object(transformer, 'logger') as mock_logger:
            transformer.process_batch([msg])
            assert mock_logger.error.call_count == 1
            assert transformer.ods_conn.rollback.call_count == 1
