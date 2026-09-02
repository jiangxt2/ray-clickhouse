from __future__ import annotations

from ray_clickhouse_comparison.summary import summarize_results

ZERO = "0" * 64


def _row(
    repetition: int,
    duration: float,
    *,
    status: str = "valid",
    telemetry_complete: bool = True,
) -> dict[str, object]:
    return {
        "scenario_id": "read.default.single",
        "side": "official",
        "repetition": repetition,
        "status": status,
        "correctness": {"row_count": 2, "schema_sha256": ZERO, "multiset_sha256": ZERO},
        "metrics": {
            "duration_seconds": duration,
            "label": "not-numeric",
            "telemetry_complete": telemetry_complete,
        },
    }


def test_summary_uses_only_valid_numeric_runs() -> None:
    summary = summarize_results([_row(0, 3.0), _row(1, 1.0), _row(2, 100.0, status="invalid")])
    group = summary["groups"][0]
    assert summary["status_counts"] == {"invalid": 1, "valid": 2}
    assert group["valid_runs"] == 2
    assert group["stable_correctness"] is True
    assert group["numeric_metrics"]["duration_seconds"]["median"] == 2.0
    assert group["paired_correctness_runs"] == 0
    assert group["paired_numeric_metrics"] == {}


def test_summary_exposes_pair_metrics_only_after_both_sides_match() -> None:
    official = _row(0, 3.0)
    external = {**_row(0, 1.0), "side": "external"}
    summary = summarize_results([official, external])

    assert all(group["paired_correctness_runs"] == 1 for group in summary["groups"])
    assert all("duration_seconds" in group["paired_numeric_metrics"] for group in summary["groups"])


def test_summary_excludes_incomplete_telemetry_from_pairs_and_resource_metrics() -> None:
    official = _row(0, 3.0, telemetry_complete=False)
    official["metrics"] = {
        "duration_seconds": 3.0,
        "container_peak_bytes.ray-head": 100,
        "clickhouse_peak_memory_bytes": 200,
        "query_duration_ms": 300,
        "query_count": 2,
        "telemetry_complete": False,
    }
    external = {**_row(0, 1.0), "side": "external"}

    summary = summarize_results([official, external])
    groups = {group["side"]: group for group in summary["groups"]}

    assert groups["official"]["paired_correctness_runs"] == 0
    assert groups["external"]["paired_correctness_runs"] == 0
    assert "container_peak_bytes.ray-head" not in groups["official"]["numeric_metrics"]
    assert "clickhouse_peak_memory_bytes" not in groups["official"]["numeric_metrics"]
    assert "query_duration_ms" not in groups["official"]["numeric_metrics"]
    assert "duration_seconds" not in groups["official"]["numeric_metrics"]
    assert groups["official"]["numeric_metrics"]["query_count"]["median"] == 2.0
