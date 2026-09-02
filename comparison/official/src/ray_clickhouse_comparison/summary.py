"""Derive compact summaries exclusively from schema-valid result rows."""

from __future__ import annotations

import statistics
from collections import defaultdict
from collections.abc import Iterable, Mapping
from typing import Any

_RESOURCE_METRIC_NAMES = frozenset(
    {
        "driver_process_sample_count",
        "ray_metric_sample_count",
        "ray_object_store_baseline_missing_location_count",
        "ray_object_store_baseline_missing_locations",
        "ray_object_store_measured_missing_location_count",
        "ray_object_store_measured_missing_locations",
        "resource_sample_count",
        "telemetry_complete",
        "worker_process_sample_count",
    }
)
_RESOURCE_METRIC_PREFIXES = (
    "clickhouse_container_memory_",
    "clickhouse_peak_memory_bytes",
    "comparison_container_memory_",
    "container_peak_bytes.",
    "driver_private_rss_",
    "duration_seconds",
    "head_container_memory_",
    "proxy_container_memory_",
    "query_duration_ms",
    "ray_container_memory_",
    "ray_object_store_memory.",
    "worker_container_memory_",
    "worker_private_rss_",
)


def _telemetry_complete(row: Mapping[str, Any]) -> bool:
    metrics = row.get("metrics")
    return isinstance(metrics, Mapping) and metrics.get("telemetry_complete") is True


def _is_resource_metric(name: str) -> bool:
    return name in _RESOURCE_METRIC_NAMES or name.startswith(_RESOURCE_METRIC_PREFIXES)


def _numeric_metrics(rows: Iterable[Mapping[str, Any]]) -> dict[str, dict[str, float]]:
    values: dict[str, list[float]] = defaultdict(list)
    for row in rows:
        metrics = row.get("metrics", {})
        if not isinstance(metrics, Mapping):
            continue
        for name, value in metrics.items():
            if not _telemetry_complete(row) and _is_resource_metric(str(name)):
                continue
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                continue
            values[str(name)].append(float(value))
    return {
        name: {
            "min": min(samples),
            "median": statistics.median(samples),
            "max": max(samples),
        }
        for name, samples in sorted(values.items())
    }


def summarize_results(rows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    all_rows = list(rows)
    pairs: dict[tuple[str, int], list[Mapping[str, Any]]] = defaultdict(list)
    for row in all_rows:
        pairs[(str(row.get("scenario_id", "")), int(row.get("repetition", -1)))].append(row)
    comparable_pairs: set[tuple[str, int]] = set()
    for key, pair in pairs.items():
        scenario, _ = key
        pair_identities = [row.get("correctness") for row in pair]
        if (
            len(pair) == 2
            and {row.get("side") for row in pair} == {"official", "external"}
            and all(row.get("status") == "valid" for row in pair)
            and pair_identities[0] == pair_identities[1]
            and all(_telemetry_complete(row) for row in pair)
            and not scenario.startswith("write.")
            and ".error." not in scenario
            and scenario != "contract.unknown_type"
        ):
            comparable_pairs.add(key)
    groups: dict[tuple[str, str], list[Mapping[str, Any]]] = defaultdict(list)
    status_counts: dict[str, int] = defaultdict(int)
    for row in all_rows:
        scenario = str(row.get("scenario_id", ""))
        side = str(row.get("side", ""))
        status = str(row.get("status", "unknown"))
        status_counts[status] += 1
        groups[(scenario, side)].append(row)
    summaries: list[dict[str, Any]] = []
    for (scenario, side), group in sorted(groups.items()):
        valid = [row for row in group if row.get("status") == "valid"]
        paired = [
            row for row in valid if (scenario, int(row.get("repetition", -1))) in comparable_pairs
        ]
        identities = {
            (
                row.get("correctness", {}).get("row_count"),
                row.get("correctness", {}).get("schema_sha256"),
                row.get("correctness", {}).get("multiset_sha256"),
            )
            for row in valid
            if isinstance(row.get("correctness"), Mapping)
        }
        summaries.append(
            {
                "scenario_id": scenario,
                "side": side,
                "runs": len(group),
                "valid_runs": len(valid),
                "stable_correctness": len(identities) <= 1 and bool(valid),
                "numeric_metrics": _numeric_metrics(valid),
                "paired_correctness_runs": len(paired),
                "paired_numeric_metrics": _numeric_metrics(paired),
            }
        )
    return {
        "schema_version": 1,
        "status_counts": dict(sorted(status_counts.items())),
        "groups": summaries,
    }
