from __future__ import annotations

import subprocess
import sys
import tarfile
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
FORBIDDEN = (
    "/comparison/",
    "/docker/comparison/",
    "/scripts/check_official_comparison.sh",
    "/scripts/run_official_comparison.sh",
    "/.github/workflows/official-comparison.yml",
    "/docs/official-comparison.md",
)


def _assert_clean_members(members: list[str]) -> None:
    normalized = [f"/{member}" for member in members]
    for forbidden in FORBIDDEN:
        assert all(forbidden not in member for member in normalized)


def test_root_distributions_exclude_comparison_only_content(tmp_path: Path) -> None:
    subprocess.run(
        [
            sys.executable,
            "-m",
            "build",
            "--no-isolation",
            "--outdir",
            str(tmp_path),
            str(ROOT),
        ],
        check=True,
        cwd=ROOT,
    )
    subprocess.run(
        [sys.executable, "-m", "twine", "check", *map(str, sorted(tmp_path.iterdir()))],
        check=True,
        cwd=ROOT,
    )
    wheel = next(tmp_path.glob("*.whl"))
    source = next(tmp_path.glob("*.tar.gz"))
    with zipfile.ZipFile(wheel) as archive:
        wheel_members = archive.namelist()
    with tarfile.open(source, "r:gz") as archive:
        source_members = archive.getnames()
    _assert_clean_members(wheel_members)
    _assert_clean_members(source_members)
    assert any(member.endswith("ray_clickhouse/py.typed") for member in wheel_members)
    assert any(member.endswith("src/ray_clickhouse/py.typed") for member in source_members)


def test_controller_wheel_starts_fault_proxy_without_runtime_dependencies(tmp_path: Path) -> None:
    dist = tmp_path / "dist"
    subprocess.run(
        [
            sys.executable,
            "-m",
            "build",
            "--no-isolation",
            "--wheel",
            "--outdir",
            str(dist),
            str(ROOT / "comparison/official"),
        ],
        check=True,
        cwd=ROOT,
    )
    wheel = next(dist.glob("*.whl"))
    subprocess.run(
        [
            sys.executable,
            "-I",
            "-S",
            "-c",
            (
                "import sys; "
                f"sys.path.insert(0, {str(wheel)!r}); "
                "import ray_clickhouse_comparison.faults; "
                "import ray_clickhouse_comparison.metrics"
            ),
        ],
        check=True,
        cwd=ROOT,
        env={"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8"},
    )
