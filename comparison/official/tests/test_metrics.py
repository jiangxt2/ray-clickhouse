from __future__ import annotations

import os
from argparse import Namespace
from pathlib import Path

import pyarrow as pa
import pytest

from ray_clickhouse_comparison.__main__ import _resource_summary
from ray_clickhouse_comparison.metrics import (
    block_metrics,
    parse_prometheus,
    parse_size_bytes,
    sample_process_tree,
    summarize_docker_stats,
    summarize_process_samples,
    summarize_ray_metric_samples,
)


def _fake_process(root: Path, pid: int, parent: int, rss_pages: int, shared_pages: int = 0) -> None:
    directory = root / str(pid)
    directory.mkdir()
    directory.joinpath("stat").write_text(
        f"{pid} (worker) S {parent} 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0\n",
        encoding="utf-8",
    )
    directory.joinpath("statm").write_text(
        f"0 {rss_pages} {shared_pages} 0 0 0 0\n",
        encoding="utf-8",
    )


def test_process_tree_rss_is_aggregated(tmp_path: Path) -> None:
    _fake_process(tmp_path, 10, 1, 10, 2)
    _fake_process(tmp_path, 11, 10, 5, 1)
    _fake_process(tmp_path, 20, 1, 20)

    sample = sample_process_tree([10], proc_root=tmp_path)
    page_size = int(os.sysconf("SC_PAGE_SIZE"))
    assert sample.aggregate_rss_bytes == 15 * page_size
    assert sample.aggregate_shared_bytes == 3 * page_size
    assert sample.aggregate_private_rss_bytes == 12 * page_size
    assert sample.per_process_rss_bytes == {10: 10 * page_size, 11: 5 * page_size}

    excluded = sample_process_tree([10], proc_root=tmp_path, exclude_pids=[11])
    assert excluded.per_process_rss_bytes == {10: 10 * page_size}


def test_prometheus_parser_filters_names_and_preserves_labels() -> None:
    values = parse_prometheus(
        """
# HELP ray_object_store_memory bytes
ray_object_store_memory{node="a"} 12
ray_spilled_bytes 4.5e2
ignored 9
""",
        names={"ray_object_store_memory", "ray_spilled_bytes"},
    )
    assert values == {
        'ray_object_store_memory{node="a"}': 12.0,
        "ray_spilled_bytes": 450.0,
    }


def test_block_metrics_report_count_rows_and_maxima() -> None:
    blocks = [pa.table({"id": [1, 2]}), pa.record_batch({"id": [3]})]
    metrics = block_metrics(blocks)
    assert metrics["block_count"] == 2
    assert metrics["total_rows"] == 3
    assert metrics["max_block_rows"] == 2
    assert metrics["total_bytes"] >= metrics["max_block_bytes"] > 0


def test_resource_summaries_preserve_sample_aligned_peaks(tmp_path: Path) -> None:
    docker_stats = tmp_path / "docker.jsonl"
    docker_stats.write_text(
        """
{"sample_index":0}
{"Name":"project-ray-worker-1-1","MemUsage":"100MiB / 2GiB"}
{"Name":"project-ray-worker-2-1","MemUsage":"200MiB / 2GiB"}
{"Name":"project-ray-head-1","MemUsage":"50MiB / 4GiB"}
{"Name":"project-runner-1","MemUsage":"25MiB / 2GiB"}
{"Name":"project-clickhouse-1","MemUsage":"100MiB / 2GiB"}
{"Name":"project-proxy-1","MemUsage":"10MiB / 2GiB"}
{"sample_index":1}
{"Name":"project-ray-worker-1-1","MemUsage":"250MiB / 2GiB"}
{"Name":"project-ray-worker-2-1","MemUsage":"50MiB / 2GiB"}
{"Name":"project-ray-head-1","MemUsage":"60MiB / 4GiB"}
{"Name":"project-runner-1","MemUsage":"30MiB / 2GiB"}
{"Name":"project-clickhouse-1","MemUsage":"110MiB / 2GiB"}
{"Name":"project-proxy-1","MemUsage":"12MiB / 2GiB"}
""".lstrip(),
        encoding="utf-8",
    )
    ray_metrics = tmp_path / "ray.prom"
    ray_metrics.write_text(
        """
# comparison_sample 0 service ray-head
ray_object_store_memory{Location="MMAP_SHM"} 0
ray_object_store_memory{Location="MMAP_DISK"} 0
ray_object_store_memory{Location="SPILLED"} 0
ray_object_store_memory{Location="WORKER_HEAP"} 0
# comparison_sample 0 service ray-worker-1
ray_object_store_memory{Location="MMAP_SHM"} 10
ray_object_store_memory{Location="MMAP_DISK"} 0
ray_object_store_memory{Location="SPILLED"} 0
ray_object_store_memory{Location="WORKER_HEAP"} 0
# comparison_sample 0 service ray-worker-2
ray_object_store_memory{Location="MMAP_SHM"} 20
ray_object_store_memory{Location="MMAP_DISK"} 0
ray_object_store_memory{Location="SPILLED"} 0
ray_object_store_memory{Location="WORKER_HEAP"} 0
# comparison_sample 1 service ray-head
ray_object_store_memory{Location="MMAP_SHM"} 0
ray_object_store_memory{Location="MMAP_DISK"} 0
ray_object_store_memory{Location="SPILLED"} 5
ray_object_store_memory{Location="WORKER_HEAP"} 0
# comparison_sample 1 service ray-worker-1
ray_object_store_memory{Location="MMAP_SHM"} 40
ray_object_store_memory{Location="MMAP_DISK"} 0
ray_object_store_memory{Location="SPILLED"} 0
ray_object_store_memory{Location="WORKER_HEAP"} 0
# comparison_sample 1 service ray-worker-2
ray_object_store_memory{Location="MMAP_SHM"} 0
ray_object_store_memory{Location="MMAP_DISK"} 0
ray_object_store_memory{Location="SPILLED"} 0
ray_object_store_memory{Location="WORKER_HEAP"} 0
""".lstrip(),
        encoding="utf-8",
    )

    docker = summarize_docker_stats(docker_stats)
    ray = summarize_ray_metric_samples(ray_metrics)

    assert parse_size_bytes("1.5GiB") == round(1.5 * 1024**3)
    assert docker["comparison_container_memory_peak_bytes"] == 512 * 1024**2
    assert docker["ray_container_memory_peak_bytes"] == 360 * 1024**2
    assert docker["worker_container_memory_peak_bytes"] == 300 * 1024**2
    assert docker["worker_container_memory_baseline_bytes"] == 300 * 1024**2
    assert docker["clickhouse_container_memory_peak_bytes"] == 110 * 1024**2
    assert "driver_container_memory_peak_bytes" not in docker
    assert docker["container_peak_bytes.project-ray-worker-1-1"] == 250 * 1024**2
    assert ray["ray_object_store_memory.MMAP_SHM_peak_bytes"] == 40.0
    assert ray["ray_object_store_memory.SPILLED_peak_bytes"] == 5.0


def test_ray_metric_summary_rejects_missing_service_or_location(tmp_path: Path) -> None:
    metrics = tmp_path / "ray.prom"
    metrics.write_text(
        "\n".join(
            [
                "# comparison_sample 0 service ray-head",
                'ray_object_store_memory{Location="MMAP_SHM"} 0',
                "# comparison_sample 1 service ray-head",
                'ray_object_store_memory{Location="MMAP_SHM"} 0',
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="required service"):
        summarize_ray_metric_samples(metrics)


def test_ray_metric_summary_records_sparse_idle_baseline(tmp_path: Path) -> None:
    metrics = tmp_path / "ray.prom"
    lines: list[str] = []
    services = ("ray-head", "ray-worker-1", "ray-worker-2")
    locations = ("MMAP_SHM", "MMAP_DISK", "SPILLED", "WORKER_HEAP")
    for sample in (0, 1):
        for service in services:
            lines.append(f"# comparison_sample {sample} service {service}")
            for location in locations:
                if sample == 0 and location.startswith("MMAP"):
                    continue
                lines.append(f'ray_object_store_memory{{Location="{location}"}} 0')
    metrics.write_text("\n".join(lines) + "\n", encoding="utf-8")

    summary = summarize_ray_metric_samples(metrics)
    assert summary["ray_object_store_baseline_missing_location_count"] == 6.0
    assert "ray-worker-1:MMAP_SHM" in summary["ray_object_store_baseline_missing_locations"]


def test_ray_metric_summary_rejects_missing_measured_service_location(tmp_path: Path) -> None:
    metrics = tmp_path / "ray.prom"
    services = ("ray-head", "ray-worker-1", "ray-worker-2")
    locations = ("MMAP_SHM", "MMAP_DISK", "SPILLED", "WORKER_HEAP")
    lines: list[str] = []
    for sample in (0, 1):
        for service in services:
            lines.append(f"# comparison_sample {sample} service {service}")
            for location in locations:
                if sample == 1 and service == "ray-worker-1" and location == "SPILLED":
                    continue
                lines.append(f'ray_object_store_memory{{Location="{location}"}} 0')
    metrics.write_text("\n".join(lines) + "\n", encoding="utf-8")

    with pytest.raises(ValueError, match="service/location data"):
        summarize_ray_metric_samples(metrics)


def test_ray_metric_summary_allows_missing_dynamic_worker_heap(tmp_path: Path) -> None:
    metrics = tmp_path / "ray.prom"
    services = ("ray-head", "ray-worker-1", "ray-worker-2")
    required_locations = ("MMAP_SHM", "MMAP_DISK", "SPILLED")
    lines: list[str] = []
    for sample in (0, 1):
        for service in services:
            lines.append(f"# comparison_sample {sample} service {service}")
            for location in required_locations:
                lines.append(f'ray_object_store_memory{{Location="{location}"}} 0')
            if not (sample == 1 and service == "ray-worker-1"):
                lines.append('ray_object_store_memory{Location="WORKER_HEAP"} 0')
    metrics.write_text("\n".join(lines) + "\n", encoding="utf-8")

    summary = summarize_ray_metric_samples(metrics)

    assert summary["ray_object_store_measured_missing_dynamic_location_count"] == 1.0
    assert (
        "sample=1 service=ray-worker-1 location=WORKER_HEAP"
        in summary["ray_object_store_measured_missing_dynamic_locations"]
    )
    assert summary["ray_object_store_memory.WORKER_HEAP_peak_bytes"] == 0.0


def test_resource_summary_does_not_accept_sparse_baseline_as_complete(tmp_path: Path) -> None:
    docker_stats = tmp_path / "docker.jsonl"
    rows = {
        "ray-worker-1-1": "1MiB",
        "ray-worker-2-1": "1MiB",
        "ray-head-1": "1MiB",
        "runner-1": "1MiB",
        "clickhouse-1": "1MiB",
        "proxy-1": "1MiB",
    }
    docker_stats.write_text(
        "".join(
            f'{{"sample_index":{sample}}}\n'
            + "".join(
                f'{{"Name":"project-{name}","MemUsage":"{memory} / 2GiB"}}\n'
                for name, memory in rows.items()
            )
            for sample in (0, 1)
        ),
        encoding="utf-8",
    )
    process_samples = tmp_path / "process.jsonl"
    process_samples.write_text(
        "\n".join(
            f'{{"sample_index":{sample},"service":"{service}","aggregate_private_rss_bytes":1}}'
            for sample in (0, 1)
            for service in ("ray-worker-1", "ray-worker-2", "driver")
        )
        + "\n",
        encoding="utf-8",
    )
    ray_metrics = tmp_path / "ray.prom"
    lines: list[str] = []
    services = ("ray-head", "ray-worker-1", "ray-worker-2")
    locations = ("MMAP_SHM", "MMAP_DISK", "SPILLED", "WORKER_HEAP")
    for sample in (0, 1):
        for service in services:
            lines.append(f"# comparison_sample {sample} service {service}")
            for location in locations:
                if sample == 0 and location != "SPILLED":
                    continue
                lines.append(f'ray_object_store_memory{{Location="{location}"}} 0')
    ray_metrics.write_text("\n".join(lines) + "\n", encoding="utf-8")

    output = tmp_path / "resources.json"
    args = Namespace(
        docker_stats=docker_stats,
        process_samples=process_samples,
        ray_metrics=ray_metrics,
        require_complete=False,
        output=output,
    )
    _resource_summary(args)
    assert '"telemetry_complete": false' in output.read_text(encoding="utf-8")

    args.require_complete = True
    with pytest.raises(ValueError, match="missing locations"):
        _resource_summary(args)


def test_process_sample_summary_requires_both_workers_per_sample(tmp_path: Path) -> None:
    samples = tmp_path / "process.jsonl"
    samples.write_text(
        "\n".join(
            [
                '{"sample_index":0,"service":"ray-worker-1","aggregate_private_rss_bytes":10}',
                '{"sample_index":0,"service":"ray-worker-2","aggregate_private_rss_bytes":20}',
                '{"sample_index":0,"service":"driver","aggregate_private_rss_bytes":5}',
                '{"sample_index":1,"service":"ray-worker-1","aggregate_private_rss_bytes":40}',
                '{"sample_index":1,"service":"ray-worker-2","aggregate_private_rss_bytes":30}',
                '{"sample_index":1,"service":"driver","aggregate_private_rss_bytes":8}',
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    summary = summarize_process_samples(samples)
    assert summary["worker_private_rss_baseline_bytes"] == 30
    assert summary["worker_private_rss_peak_bytes"] == 70
    assert summary["worker_private_rss_peak_delta_bytes"] == 40
    assert summary["driver_private_rss_peak_bytes"] == 8
