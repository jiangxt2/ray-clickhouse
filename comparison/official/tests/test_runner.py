from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from ray_clickhouse_comparison.config import load_reference, load_scenarios
from ray_clickhouse_comparison.runner import (
    _PERMISSION_USER,
    _consume,
    _exception_record,
    _exception_type_names,
    _expected_worker_loss,
    _external_read,
    _official_read,
    _setup_permission_fixture,
    _task_metrics,
    _worker_kill_marker,
    _write_dataset,
    classify_query_role,
    validate_smoke_result,
)

ROOT = Path(__file__).resolve().parents[1]


def _scenario(scenario_id: str):
    return next(
        scenario
        for scenario in load_scenarios(ROOT / "config/scenarios.toml")
        if scenario.id == scenario_id
    )


class _Dataset:
    def __init__(self) -> None:
        self.materialized = False

    def materialize(self) -> _Dataset:
        self.materialized = True
        return self

    def iter_batches(
        self,
        *,
        batch_size: int | None,
        prefetch_batches: int,
        batch_format: str,
    ):
        assert batch_size is None
        assert prefetch_batches == 0
        assert batch_format == "pyarrow"
        yield pa.table({"id": [1, 2]})
        yield pa.record_batch({"id": [3]})


def test_consume_distinguishes_materialization_and_preserves_blocks() -> None:
    dataset = _Dataset()
    identity, metrics = _consume(dataset, "materialize")
    assert dataset.materialized is True
    assert identity["row_count"] == 3
    assert metrics["block_count"] == 2


def test_exception_record_preserves_public_chain_without_repr() -> None:
    error = ValueError("failure")
    record = _exception_record(error)
    assert record["type"] == "ValueError"
    assert record["module"] == "builtins"
    assert record["message"] == "failure"
    assert "ValueError: failure" in record["traceback"]


def test_exception_chain_preserves_wrapped_public_type() -> None:
    cause = ValueError("cause")
    wrapper = RuntimeError("wrapper")
    wrapper.__cause__ = cause
    assert _exception_type_names(wrapper) == ("RuntimeError", "ValueError")


def test_local_smoke_rejects_correctness_difference_but_formal_preserves_it() -> None:
    document = {
        "status": "invalid",
        "metrics": {
            "query_count": 3,
            "unclassified_query_count": 0,
            "unexpected_query_roles": "",
        },
    }
    scenario = _scenario("read.default.single")
    with pytest.raises(ValueError, match="correctness"):
        validate_smoke_result(document, scenario, "smoke")
    validate_smoke_result(document, scenario, "formal")


def test_smoke_result_requires_proven_fault_boundary() -> None:
    with pytest.raises(ValueError, match="fault boundary"):
        validate_smoke_result(
            {"status": "invalid", "metrics": {"fault_observed": False}},
            _scenario("write.transport.post_commit"),
            "smoke",
        )


def test_expected_error_result_is_an_observation_not_infrastructure_failure() -> None:
    validate_smoke_result(
        {"status": "invalid", "metrics": {"outcome": "empty_dataset"}, "exception": None},
        _scenario("read.error.permission"),
        "formal",
    )


@pytest.mark.parametrize(
    ("query", "role"),
    [
        ("DESCRIBE TABLE t", "planning"),
        ("SELECT engine FROM system.tables", "planning"),
        ("EXISTS TABLE t", "planning"),
        ("EXPLAIN SELECT 1 FROM t", "planning"),
        ("SELECT * FROM t LIMIT 0", "planning"),
        ("SELECT COUNT(*) FROM t", "estimate"),
        ("SELECT * FROM t LIMIT 100", "sample"),
        ("INSERT INTO t FORMAT Arrow", "data"),
        ("SELECT * FROM t", "data"),
    ],
)
def test_query_roles_are_predeclared(query: str, role: str) -> None:
    assert classify_query_role(query) == role


def test_default_and_controlled_public_read_arguments_are_distinct(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    class FakeRayData:
        @staticmethod
        def read_clickhouse(**kwargs):
            calls.append(kwargs)
            return object()

    class FakeExternal:
        @staticmethod
        def read_clickhouse(**kwargs):
            calls.append(kwargs)
            return object()

    reference = load_reference(ROOT / "config/reference.toml")
    monkeypatch.setenv("RAY_COMPARISON_CLICKHOUSE_PROXY_HOST", "proxy")
    monkeypatch.setenv("RAY_COMPARISON_CLICKHOUSE_PROXY_PORT", "18123")
    monkeypatch.setattr(
        "ray_clickhouse_comparison.runner.importlib.import_module",
        lambda name: FakeRayData if name == "ray.data" else FakeExternal,
    )

    _official_read("db", "table", _scenario("read.default.single"), reference, "tag")
    default_official = calls.pop()
    _external_read("db", "table", _scenario("read.default.single"), reference, "tag")
    default_external = calls.pop()
    _official_read("db", "table", _scenario("read.controlled.ordered"), reference, "tag")
    controlled_official = calls.pop()
    _external_read("db", "table", _scenario("read.controlled.ordered"), reference, "tag")
    controlled_external = calls.pop()
    _official_read("db", "table", _scenario("contract.column_order"), reference, "tag")
    ordered_official = calls.pop()
    _external_read("db", "table", _scenario("contract.column_order"), reference, "tag")
    ordered_external = calls.pop()

    assert "concurrency" not in default_official
    assert "concurrency" not in default_external
    assert controlled_official["concurrency"] == 2
    assert controlled_external["split"] == "range"
    assert controlled_external["batch_rows"] == 65536
    assert controlled_official["client_kwargs"]["connect_timeout"] == 300
    assert controlled_external["connect_timeout_seconds"] == 300
    assert ordered_official["columns"] == ["payload", "id"]
    assert ordered_external["columns"] == ["payload", "id"]


def test_permission_fixture_is_fixed_no_password_and_no_select(monkeypatch) -> None:
    commands: list[str] = []

    class FakeClient:
        def command(self, query: str) -> None:
            commands.append(query)

        def close(self) -> None:
            pass

    monkeypatch.setattr("ray_clickhouse_comparison.runner._direct_client", lambda: FakeClient())
    _setup_permission_fixture()

    assert _PERMISSION_USER == "ray_clickhouse_comparison_no_select"
    assert commands == [
        f"DROP USER IF EXISTS `{_PERMISSION_USER}`",
        f"CREATE USER `{_PERMISSION_USER}` IDENTIFIED WITH no_password",
    ]


def test_worker_loss_write_requests_a_worker_task(monkeypatch) -> None:
    calls: dict[str, object] = {}

    class FakeDataset:
        def repartition(self, count: int) -> FakeDataset:
            assert count == 1
            return self

        def write_clickhouse(self, *args, **kwargs) -> None:
            calls["official"] = kwargs

    class FakeRayData:
        SinkMode = type("SinkMode", (), {"APPEND": "append"})

        @staticmethod
        def from_arrow(table):
            assert table.num_rows == 2
            return FakeDataset()

    class FakeExternal:
        @staticmethod
        def write_clickhouse(dataset, **kwargs) -> None:
            assert isinstance(dataset, FakeDataset)
            calls["external"] = kwargs

    reference = load_reference(ROOT / "config/reference.toml")
    monkeypatch.setenv("RAY_COMPARISON_CLICKHOUSE_PROXY_HOST", "proxy")
    monkeypatch.setenv("RAY_COMPARISON_CLICKHOUSE_PROXY_PORT", "18123")
    monkeypatch.setattr(
        "ray_clickhouse_comparison.runner.importlib.import_module",
        lambda name: FakeRayData if name == "ray.data" else FakeExternal,
    )

    _write_dataset(
        "official",
        "db",
        "table",
        "tag",
        _scenario("write.worker.post_commit"),
        reference,
    )
    _write_dataset(
        "external",
        "db",
        "table",
        "tag",
        _scenario("write.worker.post_commit"),
        reference,
    )

    assert calls["official"]["ray_remote_args"] == {"num_cpus": 1}
    assert calls["external"]["ray_remote_args"] == {"num_cpus": 1}


def test_worker_loss_contract_distinguishes_public_retry_policies() -> None:
    default = _scenario("write.worker.post_commit")
    zero = _scenario("write.worker.controlled_zero_retry")
    assert _expected_worker_loss("official", default) == (2, 1, 1, "FAILED,FINISHED", 4, 4)
    assert _expected_worker_loss("official", zero) == (1, 1, 0, "FAILED", 2, 2)
    assert _expected_worker_loss("external", default) == (1, 1, 0, "FAILED", 1, 2)
    assert _expected_worker_loss("external", zero) == (1, 1, 0, "FAILED", 1, 2)


def test_worker_loss_uses_explicit_case_marker(monkeypatch, tmp_path: Path) -> None:
    control_dir = tmp_path / "control"
    configured = tmp_path / "case" / "killed-worker.txt"
    monkeypatch.setenv("RAY_COMPARISON_WORKER_KILL_MARKER", str(configured))

    assert _worker_kill_marker(control_dir) == configured


def test_worker_loss_marker_defaults_next_to_control_dir(tmp_path: Path) -> None:
    control_dir = tmp_path / "case" / "control"

    assert _worker_kill_marker(control_dir) == tmp_path / "case" / "killed-worker.txt"


def test_task_metrics_scopes_write_evidence_to_write_tasks(monkeypatch) -> None:
    class Task:
        def __init__(self, name: str, state: str, task_id: str, attempt: int) -> None:
            self.name = name
            self.state = state
            self.task_id = task_id
            self.attempt_number = attempt
            self.type = "NORMAL_TASK"
            self.node_id = "node"
            self.worker_id = "worker"

    tasks = [
        Task("get_table_block_metadata_schema", "FINISHED", "metadata", 0),
        Task("Write", "FAILED", "write", 0),
        Task("Write", "FINISHED", "write", 1),
        Task("reduce", "FINISHED", "reduce", 0),
    ]

    class FakeRay:
        @staticmethod
        def get_runtime_context():
            return type("Context", (), {"get_job_id": lambda self: "job"})()

    class FakeState:
        @staticmethod
        def list_tasks(**kwargs):
            assert kwargs["filters"] == [("job_id", "=", "job")]
            return tasks

    def import_module(name: str):
        return FakeRay if name == "ray" else FakeState

    monkeypatch.setattr("ray_clickhouse_comparison.runner.importlib.import_module", import_module)
    metrics = _task_metrics(None, task_name="Write")
    assert metrics["ray_task_attempt_count"] == 2
    assert metrics["ray_failed_task_attempt_count"] == 1
    assert metrics["ray_task_states"] == "FAILED,FINISHED"
