"""Small, dependency-free collectors for process, block, and Prometheus metrics."""

from __future__ import annotations

import argparse
import json
import os
import re
import time
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_PROMETHEUS = re.compile(
    r"^(?P<name>[a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(?P<labels>[^}]*)\})?\s+(?P<value>[-+0-9.eE]+)$"
)
_SIZE = re.compile(r"^([0-9]+(?:\.[0-9]+)?)\s*([KMGT]?i?B)$", re.IGNORECASE)
_SIZE_MULTIPLIERS = {
    "b": 1,
    "kb": 1000,
    "kib": 1024,
    "mb": 1000**2,
    "mib": 1024**2,
    "gb": 1000**3,
    "gib": 1024**3,
    "tb": 1000**4,
    "tib": 1024**4,
}


@dataclass(frozen=True)
class ProcessSample:
    timestamp: float
    aggregate_rss_bytes: int
    aggregate_shared_bytes: int
    aggregate_private_rss_bytes: int
    per_process_rss_bytes: dict[int, int]


def _read_parent_pid(pid: int, proc_root: Path) -> int | None:
    try:
        value = (proc_root / str(pid) / "stat").read_text(encoding="utf-8")
        fields = value.rsplit(")", 1)[1].split()
        return int(fields[1])
    except (FileNotFoundError, IndexError, ValueError, PermissionError):
        return None


def _read_memory(pid: int, proc_root: Path) -> tuple[int, int] | None:
    try:
        fields = (proc_root / str(pid) / "statm").read_text(encoding="utf-8").split()
        page_size = int(os.sysconf("SC_PAGE_SIZE"))
        return int(fields[1]) * page_size, int(fields[2]) * page_size
    except (FileNotFoundError, IndexError, ValueError, PermissionError):
        return None


def process_tree_pids(
    roots: Iterable[int],
    *,
    proc_root: Path = Path("/proc"),
    exclude_pids: Iterable[int] = (),
) -> set[int]:
    root_set = set(roots)
    selected = set(root_set)
    parents: dict[int, int] = {}
    try:
        candidates = [int(path.name) for path in proc_root.iterdir() if path.name.isdigit()]
    except (FileNotFoundError, PermissionError):
        return selected - set(exclude_pids)
    for pid in candidates:
        parent = _read_parent_pid(pid, proc_root)
        if parent is not None:
            parents[pid] = parent
    changed = True
    while changed:
        changed = False
        for pid, parent in parents.items():
            if parent in selected and pid not in selected:
                selected.add(pid)
                changed = True
    return selected - set(exclude_pids)


def sample_process_tree(
    roots: Iterable[int],
    *,
    proc_root: Path = Path("/proc"),
    exclude_pids: Iterable[int] = (),
) -> ProcessSample:
    rss_by_pid: dict[int, int] = {}
    shared_by_pid: dict[int, int] = {}
    for pid in process_tree_pids(roots, proc_root=proc_root, exclude_pids=exclude_pids):
        memory = _read_memory(pid, proc_root)
        if memory is not None:
            rss_by_pid[pid], shared_by_pid[pid] = memory
    aggregate_rss = sum(rss_by_pid.values())
    aggregate_shared = sum(shared_by_pid.values())
    return ProcessSample(
        time.time(),
        aggregate_rss,
        aggregate_shared,
        max(0, aggregate_rss - aggregate_shared),
        rss_by_pid,
    )


def parse_prometheus(text: str, *, names: set[str] | None = None) -> dict[str, float]:
    values: dict[str, float] = {}
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        match = _PROMETHEUS.fullmatch(line)
        if match is None:
            continue
        name = match.group("name")
        if names is not None and name not in names:
            continue
        labels = match.group("labels") or ""
        key = name if not labels else f"{name}{{{labels}}}"
        values[key] = float(match.group("value"))
    return values


def block_metrics(blocks: Iterable[Any]) -> dict[str, int]:
    import pyarrow as pa

    block_count = 0
    total_rows = 0
    total_bytes = 0
    max_rows = 0
    max_bytes = 0
    for block in blocks:
        table = pa.Table.from_batches([block]) if isinstance(block, pa.RecordBatch) else block
        block_count += 1
        total_rows += table.num_rows
        total_bytes += table.nbytes
        max_rows = max(max_rows, table.num_rows)
        max_bytes = max(max_bytes, table.nbytes)
    return {
        "block_count": block_count,
        "total_rows": total_rows,
        "total_bytes": total_bytes,
        "max_block_rows": max_rows,
        "max_block_bytes": max_bytes,
    }


def parse_size_bytes(value: str) -> int:
    match = _SIZE.fullmatch(value.strip())
    if match is None:
        raise ValueError(f"unsupported size value: {value!r}")
    return round(float(match.group(1)) * _SIZE_MULTIPLIERS[match.group(2).lower()])


def summarize_docker_stats(path: Path) -> dict[str, int]:
    categories = ("all", "ray", "worker", "head", "clickhouse", "proxy")
    by_category: dict[str, dict[int, int]] = {category: {} for category in categories}
    per_container_peak: dict[str, int] = {}
    rows_by_sample: dict[int, int] = {}
    sample = -1
    for line in path.read_text(encoding="utf-8").splitlines():
        value = json.loads(line)
        if "sample_index" in value:
            sample = int(value["sample_index"])
            for values in by_category.values():
                values.setdefault(sample, 0)
            rows_by_sample.setdefault(sample, 0)
            continue
        if sample < 0:
            raise ValueError("Docker stats row appears before its sample marker")
        name = str(value["Name"])
        memory = parse_size_bytes(str(value["MemUsage"]).split("/", 1)[0])
        by_category["all"][sample] += memory
        rows_by_sample[sample] += 1
        if "ray-worker" in name:
            by_category["worker"][sample] += memory
            by_category["ray"][sample] += memory
        elif "ray-head" in name:
            by_category["head"][sample] += memory
            by_category["ray"][sample] += memory
        elif "clickhouse" in name:
            by_category["clickhouse"][sample] += memory
        elif "proxy" in name:
            by_category["proxy"][sample] += memory
        per_container_peak[name] = max(per_container_peak.get(name, 0), memory)
    aggregate_by_sample = by_category["all"]
    if not aggregate_by_sample:
        raise ValueError("no Docker resource samples were collected")
    if len(aggregate_by_sample) < 2:
        raise ValueError("Docker resource evidence has no measured sample after baseline")
    if any(count == 0 for count in rows_by_sample.values()):
        raise ValueError("a Docker resource sample contains no container rows")
    if any(any(value == 0 for value in samples.values()) for samples in by_category.values()):
        raise ValueError("a Docker resource sample is missing a required service category")
    baseline_sample = min(aggregate_by_sample)
    result = {
        "comparison_container_memory_peak_bytes": max(aggregate_by_sample.values()),
        "comparison_container_memory_baseline_bytes": aggregate_by_sample[baseline_sample],
        "resource_sample_count": len(aggregate_by_sample),
    }
    for category in categories[1:]:
        values = by_category[category]
        result[f"{category}_container_memory_peak_bytes"] = max(values.values())
        result[f"{category}_container_memory_baseline_bytes"] = values[baseline_sample]
        result[f"{category}_container_memory_peak_delta_bytes"] = (
            max(values.values()) - values[baseline_sample]
        )
    result.update(
        {
            f"container_peak_bytes.{name}": value
            for name, value in sorted(per_container_peak.items())
        }
    )
    return result


def summarize_process_samples(path: Path) -> dict[str, int]:
    worker_by_sample: dict[int, int] = {}
    driver_by_sample: dict[int, int] = {}
    per_worker_peak: dict[str, int] = {}
    workers_by_sample: dict[int, int] = {}
    drivers_by_sample: dict[int, int] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        value = json.loads(line)
        sample = int(value["sample_index"])
        service = str(value["service"])
        private_rss = int(value["aggregate_private_rss_bytes"])
        if service == "driver":
            driver_by_sample[sample] = driver_by_sample.get(sample, 0) + private_rss
            drivers_by_sample[sample] = drivers_by_sample.get(sample, 0) + 1
        elif service.startswith("ray-worker-"):
            worker_by_sample[sample] = worker_by_sample.get(sample, 0) + private_rss
            workers_by_sample[sample] = workers_by_sample.get(sample, 0) + 1
            per_worker_peak[service] = max(per_worker_peak.get(service, 0), private_rss)
        else:
            raise ValueError(f"unknown process sample service: {service}")
    if not worker_by_sample:
        raise ValueError("no worker process samples were collected")
    if len(worker_by_sample) < 2:
        raise ValueError("worker process evidence has no measured sample after baseline")
    if any(count != 2 for count in workers_by_sample.values()):
        raise ValueError("worker process samples must contain both workers")
    if not driver_by_sample:
        raise ValueError("no driver process samples were collected")
    if any(count != 1 for count in drivers_by_sample.values()):
        raise ValueError("driver process samples must contain one driver per sample")
    if set(driver_by_sample) != set(worker_by_sample):
        raise ValueError("driver and worker process samples are not sample-aligned")
    baseline_sample = min(worker_by_sample)
    driver_baseline_sample = min(driver_by_sample)
    result = {
        "worker_private_rss_peak_bytes": max(worker_by_sample.values()),
        "worker_private_rss_baseline_bytes": worker_by_sample[baseline_sample],
        "worker_private_rss_peak_delta_bytes": max(worker_by_sample.values())
        - worker_by_sample[baseline_sample],
        "worker_process_sample_count": len(worker_by_sample),
        "driver_private_rss_peak_bytes": max(driver_by_sample.values()),
        "driver_private_rss_baseline_bytes": driver_by_sample[driver_baseline_sample],
        "driver_private_rss_peak_delta_bytes": max(driver_by_sample.values())
        - driver_by_sample[driver_baseline_sample],
        "driver_process_sample_count": len(driver_by_sample),
    }
    result.update(
        {
            f"worker_private_rss_peak_bytes.{service}": value
            for service, value in sorted(per_worker_peak.items())
        }
    )
    return result


def _label_value(key: str, label: str) -> str | None:
    match = re.search(rf'(?:\{{|,)\s*{re.escape(label)}="((?:[^"\\]|\\.)*)"', key)
    return match.group(1) if match is not None else None


def summarize_ray_metric_samples(
    path: Path, *, allow_incomplete: bool = False
) -> dict[str, float | str]:
    expected_services = {"ray-head", "ray-worker-1", "ray-worker-2"}
    expected_locations = {"MMAP_SHM", "MMAP_DISK", "SPILLED", "WORKER_HEAP"}
    selected = {"ray_object_store_memory"}
    totals: dict[tuple[int, str], float] = {}
    services_by_sample: dict[int, set[str]] = {}
    locations_by_sample_service: dict[tuple[int, str], set[str]] = {}
    sample = -1
    service: str | None = None
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith("# comparison_sample "):
            fields = line.split()
            if len(fields) != 5 or fields[3] != "service":
                raise ValueError("malformed Ray metric sample marker")
            sample = int(fields[2])
            service = fields[4]
            if service not in expected_services:
                raise ValueError(f"unexpected Ray metric sample service: {service}")
            services_by_sample.setdefault(sample, set()).add(service)
            continue
        if sample < 0 or service is None:
            continue
        for key, value in parse_prometheus(line, names=selected).items():
            name = key.split("{", 1)[0]
            location = _label_value(key, "Location")
            if location not in expected_locations:
                raise ValueError("Ray Object Store metric has an unknown or missing location")
            locations_by_sample_service.setdefault((sample, service), set()).add(location)
            totals[(sample, f"{name}.{location}")] = (
                totals.get((sample, f"{name}.{location}"), 0.0) + value
            )
    if not totals:
        raise ValueError("no Ray Object Store metrics were collected")
    sample_ids = set(services_by_sample)
    if len(sample_ids) < 2:
        raise ValueError("Ray Object Store evidence has no measured sample after baseline")
    if any(services_by_sample[sample] != expected_services for sample in sample_ids):
        raise ValueError("Ray Object Store evidence is missing a required service")
    baseline_sample = min(sample_ids)
    measured_samples = sample_ids - {baseline_sample}
    if not measured_samples:
        raise ValueError("Ray Object Store evidence has no measured sample after baseline")
    measured_missing = {
        (sample, service, location)
        for sample in measured_samples
        for service in expected_services
        for location in expected_locations
        if location not in locations_by_sample_service.get((sample, service), set())
    }
    missing_details = ", ".join(
        f"sample={sample} service={service} location={location}"
        for sample, service, location in sorted(measured_missing)
    )
    if measured_missing and not allow_incomplete:
        raise ValueError(
            "Ray Object Store evidence is missing required service/location data: "
            f"{missing_details}"
        )
    baseline_missing = {
        (service, location)
        for service in expected_services
        for location in expected_locations
        if location not in locations_by_sample_service.get((baseline_sample, service), set())
    }
    result: dict[str, float | str] = {
        "ray_metric_sample_count": float(len(sample_ids)),
        "ray_object_store_baseline_missing_location_count": float(len(baseline_missing)),
        "ray_object_store_baseline_missing_locations": ",".join(
            f"{service}:{location}" for service, location in sorted(baseline_missing)
        ),
    }
    if measured_missing:
        result["ray_object_store_measured_missing_location_count"] = float(len(measured_missing))
        result["ray_object_store_measured_missing_locations"] = missing_details
        return result
    for location in ("MMAP_SHM", "MMAP_DISK", "SPILLED", "WORKER_HEAP"):
        metric = f"ray_object_store_memory.{location}"
        values = [value for (sample, name), value in totals.items() if name == metric]
        baseline = totals.get((baseline_sample, metric), 0.0)
        result[f"{metric}_baseline_bytes"] = baseline
        result[f"{metric}_peak_bytes"] = max(values, default=0.0)
        result[f"{metric}_peak_delta_bytes"] = max(values, default=0.0) - baseline
    return result


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root-pid", type=int, required=True)
    parser.add_argument("--service", required=True)
    parser.add_argument("--sample-index", type=int, required=True)
    args = parser.parse_args(argv)
    sample = sample_process_tree([args.root_pid], exclude_pids=[os.getpid()])
    value = {
        "timestamp": sample.timestamp,
        "aggregate_rss_bytes": sample.aggregate_rss_bytes,
        "aggregate_shared_bytes": sample.aggregate_shared_bytes,
        "aggregate_private_rss_bytes": sample.aggregate_private_rss_bytes,
        "service": args.service,
        "sample_index": args.sample_index,
    }
    print(json.dumps(value, sort_keys=True))


if __name__ == "__main__":
    main()
