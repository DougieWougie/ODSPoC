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
