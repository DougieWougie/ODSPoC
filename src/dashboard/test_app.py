import subprocess
from pathlib import Path
from unittest.mock import patch, MagicMock
import app


def _mock_proc(stdout: str) -> MagicMock:
    m = MagicMock()
    m.stdout = stdout
    return m


def test_experiment_metrics_no_file(tmp_path):
    with patch.object(app, 'EXPERIMENT_FILE', tmp_path / ".experiment"):
        result = app.experiment_metrics()
    assert result == {"experiment": None}


def test_experiment_metrics_exp_a(tmp_path):
    f = tmp_path / ".experiment"
    f.write_text("A\n")
    with patch.object(app, 'EXPERIMENT_FILE', f), \
         patch('subprocess.run', return_value=_mock_proc("infrastructure-transformer-1\n")):
        result = app.experiment_metrics()
    assert result["experiment"] == "A"
    assert result["running"] == 1
    assert result["requested"] == 1
    assert result["name"] == "Single Node Bottleneck"
    assert "detail" in result


def test_experiment_metrics_exp_b_all_running(tmp_path):
    f = tmp_path / ".experiment"
    f.write_text("B")
    names = "\n".join(f"infrastructure-transformer-{i}" for i in range(1, 11)) + "\n"
    with patch.object(app, 'EXPERIMENT_FILE', f), \
         patch('subprocess.run', return_value=_mock_proc(names)):
        result = app.experiment_metrics()
    assert result["experiment"] == "B"
    assert result["running"] == 10
    assert result["requested"] == 10


def test_experiment_metrics_exp_b_degraded(tmp_path):
    f = tmp_path / ".experiment"
    f.write_text("B")
    names = "\n".join(f"infrastructure-transformer-{i}" for i in range(1, 9)) + "\n"
    with patch.object(app, 'EXPERIMENT_FILE', f), \
         patch('subprocess.run', return_value=_mock_proc(names)):
        result = app.experiment_metrics()
    assert result["running"] == 8
    assert result["requested"] == 10


def test_experiment_metrics_exp_c(tmp_path):
    f = tmp_path / ".experiment"
    f.write_text("C")
    with patch.object(app, 'EXPERIMENT_FILE', f), \
         patch('subprocess.run', return_value=_mock_proc("")):
        result = app.experiment_metrics()
    assert result["experiment"] == "C"
    assert result["running"] == 0
    assert result["requested"] == 0
    assert result["name"] == "Data Virtualization"


def test_experiment_metrics_unknown_letter(tmp_path):
    f = tmp_path / ".experiment"
    f.write_text("Z")
    with patch.object(app, 'EXPERIMENT_FILE', f):
        result = app.experiment_metrics()
    assert result == {"experiment": None}


def test_experiment_metrics_docker_error(tmp_path):
    f = tmp_path / ".experiment"
    f.write_text("A")
    with patch.object(app, 'EXPERIMENT_FILE', f), \
         patch('subprocess.run', side_effect=Exception("docker not found")):
        result = app.experiment_metrics()
    assert result["experiment"] is None
    assert "err" in result


from fastapi.testclient import TestClient


def test_ods_records_invalid_table():
    client = TestClient(app.app)
    resp = client.get("/ods-records?table=injected_table")
    assert resp.status_code == 400
    assert "table must be one of" in resp.json()["detail"]


def test_ods_records_event():
    mock_conn = MagicMock()
    mock_cur = mock_conn.cursor.return_value
    mock_cur.description = [
        ("event_id",), ("event_type",), ("event_amount",),
        ("currency",), ("event_timestamp",), ("latency_s",),
    ]
    mock_cur.fetchall.return_value = [
        ("a3f2b1c9-0000-0000-0000-000000000001", "PAYMENT_TRANSACTION", 100.0, "GBP", None, 0.18),
    ]
    with patch("psycopg2.connect", return_value=mock_conn):
        client = TestClient(app.app)
        resp = client.get("/ods-records?table=event&limit=1")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 1
    assert data[0]["event_type"] == "PAYMENT_TRANSACTION"
    assert data[0]["latency_s"] == 0.18
    assert data[0]["currency"] == "GBP"


def test_ods_records_party():
    mock_conn = MagicMock()
    mock_cur = mock_conn.cursor.return_value
    mock_cur.description = [
        ("party_id",), ("party_type",), ("first_name",),
        ("last_name",), ("source_system",), ("integration_timestamp",),
    ]
    mock_cur.fetchall.return_value = [
        ("pid-1234", "INDIVIDUAL", "Alice", "Smith", "CORE_BANKING_CLIENT", None),
    ]
    with patch("psycopg2.connect", return_value=mock_conn):
        client = TestClient(app.app)
        resp = client.get("/ods-records?table=party&limit=1")
    assert resp.status_code == 200
    data = resp.json()
    assert data[0]["first_name"] == "Alice"
    assert data[0]["party_type"] == "INDIVIDUAL"


def test_ods_records_arrangement():
    mock_conn = MagicMock()
    mock_cur = mock_conn.cursor.return_value
    mock_cur.description = [
        ("arrangement_id",), ("product_category",), ("balance",),
        ("status",), ("source_system",), ("integration_timestamp",),
    ]
    mock_cur.fetchall.return_value = [
        ("aid-9999", "CHECKING_ACCOUNT", 5000.0, "ACTIVE", "CORE_BANKING", None),
    ]
    with patch("psycopg2.connect", return_value=mock_conn):
        client = TestClient(app.app)
        resp = client.get("/ods-records?table=arrangement&limit=1")
    assert resp.status_code == 200
    data = resp.json()
    assert data[0]["product_category"] == "CHECKING_ACCOUNT"
    assert data[0]["balance"] == 5000.0


def test_ods_records_db_error():
    with patch("psycopg2.connect", side_effect=Exception("connection refused")):
        client = TestClient(app.app)
        resp = client.get("/ods-records?table=event")
    assert resp.status_code == 500
    assert "connection refused" in resp.json()["detail"]
