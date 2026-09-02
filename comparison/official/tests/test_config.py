from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest

from ray_clickhouse_comparison.config import (
    ChangeRecord,
    classify_changed_paths,
    classify_git_range,
    load_reference,
    load_scenarios,
    parse_name_status_z,
    scenario_digest,
    write_github_classification,
)

ROOT = Path(__file__).resolve().parents[1]


def test_reference_and_scenarios_are_valid_and_stable() -> None:
    reference = load_reference(ROOT / "config/reference.toml")
    scenarios = load_scenarios(ROOT / "config/scenarios.toml")

    assert reference.runtime_base_commit == "1b1995155dd8bd68c6e537daa8b9348f80ce7c83"
    assert reference.docker.context == "."
    assert len(scenarios) == 15
    assert len(scenario_digest(scenarios)) == 64
    assert {scenario.split for scenario in scenarios} == {"single", "partition", "range"}
    assert any(scenario.write_retry == "zero" for scenario in scenarios)
    assert all(scenario.query_roles and scenario.invalid_if for scenario in scenarios)


def test_official_and_external_runtime_locks_have_identical_versions() -> None:
    pattern = re.compile(r"^([a-z0-9][a-z0-9._-]*)==([^ \\]+)")

    def versions(path: Path) -> dict[str, str]:
        return {
            match.group(1): match.group(2)
            for line in path.read_text(encoding="utf-8").splitlines()
            if (match := pattern.match(line)) is not None
        }

    official = versions(ROOT / "env/official-requirements.txt")
    external = versions(ROOT / "env/external-requirements.txt")
    assert official == external
    assert official["ray"] == "2.58.0"
    assert official["pyarrow"] == "19.0.1"
    assert official["clickhouse-connect"] == "1.5.0"


@pytest.mark.parametrize(
    "records",
    [
        (ChangeRecord("A", "comparison/official/evidence/run-20260902/results.jsonl"),),
        (
            ChangeRecord("M", "docs/official-comparison.md"),
            ChangeRecord("M", "tests/it-ledger.md"),
        ),
    ],
)
def test_closed_evidence_paths_are_noop(records: tuple[ChangeRecord, ...]) -> None:
    result = classify_changed_paths(records)
    assert result.runtime_relevant is False
    assert result.reason == "closed evidence-only allowlist"


@pytest.mark.parametrize(
    "records",
    [
        (),
        (ChangeRecord("M", "src/ray_clickhouse/_api.py"),),
        (ChangeRecord("D", "docs/official-comparison.md"),),
        (ChangeRecord("R", "comparison/official/evidence/a/results.jsonl"),),
        (ChangeRecord("A", "comparison/official/evidence.txt"),),
        (ChangeRecord("A", "comparison/official/evidence/run-20260902/unknown.json"),),
        (
            ChangeRecord("A", "comparison/official/evidence/run-20260902/results.jsonl"),
            ChangeRecord("A", "comparison/official/evidence/run-20260903/summary.json"),
        ),
    ],
)
def test_unresolved_or_unexpected_change_is_runtime_relevant(
    records: tuple[ChangeRecord, ...],
) -> None:
    assert classify_changed_paths(records).runtime_relevant is True


def test_duplicate_scenario_is_rejected(tmp_path: Path) -> None:
    scenarios = tmp_path / "scenarios.toml"
    scenarios.write_text(
        """
schema_version = 1
[[scenario]]
id = "duplicate"
group = "group"
sides = ["official", "external"]
profile = "default"
terminal_action = "stream"
fault = "none"
split = "single"
write_retry = "default"
correctness_gate = "fixture_identity"
query_roles = ["data"]
invalid_if = ["fixture_mismatch"]
fixture_rows = 1
fixture_payload_bytes = 32
repetitions = 1
[[scenario]]
id = "duplicate"
group = "group"
sides = ["official", "external"]
profile = "default"
terminal_action = "stream"
fault = "none"
split = "single"
write_retry = "default"
correctness_gate = "fixture_identity"
query_roles = ["data"]
invalid_if = ["fixture_mismatch"]
fixture_rows = 1
fixture_payload_bytes = 32
repetitions = 1
""",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="duplicate scenario"):
        load_scenarios(scenarios)


def test_nul_delimited_git_parser_handles_paths_without_shell_splitting() -> None:
    records = parse_name_status_z(
        b"A\0comparison/official/evidence/run-20260902/results.jsonl\0M\0docs/official-comparison.md\0"
    )
    assert records == (
        ChangeRecord("A", "comparison/official/evidence/run-20260902/results.jsonl"),
        ChangeRecord("M", "docs/official-comparison.md"),
    )


def test_rename_is_fail_closed_and_output_is_a_required_check_contract(tmp_path: Path) -> None:
    records = parse_name_status_z(b"R100\0docs/official-comparison.md\0docs/renamed.md\0")
    classification = classify_changed_paths(records)
    output = tmp_path / "github-output"
    write_github_classification(output, classification)

    assert classification.runtime_relevant is True
    assert "runtime_relevant=true" in output.read_text(encoding="utf-8")
    assert "changed_paths=[]" in output.read_text(encoding="utf-8")


def test_github_output_uses_single_line_json_for_adversarial_path(tmp_path: Path) -> None:
    output = tmp_path / "github-output"
    classification = classify_changed_paths(
        (
            ChangeRecord(
                "A",
                "comparison/official/evidence/run-20260902/RAY_CLICKHOUSE_CHANGED_PATHS",
            ),
        )
    )
    write_github_classification(output, classification)

    lines = output.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 3
    assert lines[2].startswith("changed_paths=[")


def test_git_range_classifier_executes_closed_allowlist_end_to_end(tmp_path: Path) -> None:
    def git(*args: str) -> str:
        result = subprocess.run(
            ["git", *args], cwd=tmp_path, check=True, capture_output=True, text=True
        )
        return result.stdout.strip()

    git("init", "-q")
    git("config", "user.name", "comparison-test")
    git("config", "user.email", "comparison-test@example.invalid")
    git("config", "commit.gpgsign", "false")
    runtime = tmp_path / "src/runtime.py"
    runtime.parent.mkdir()
    runtime.write_text("VALUE = 1\n", encoding="utf-8")
    git("add", "src/runtime.py")
    git("commit", "-qm", "base")
    base = git("rev-parse", "HEAD")
    evidence = tmp_path / "comparison/official/evidence/run-20260902/results.jsonl"
    evidence.parent.mkdir(parents=True)
    evidence.write_text("{}\n", encoding="utf-8")
    git("add", "comparison/official/evidence/run-20260902/results.jsonl")
    git("commit", "-qm", "evidence")
    evidence_head = git("rev-parse", "HEAD")

    assert classify_git_range("push", base, evidence_head, tmp_path).runtime_relevant is False

    runtime.write_text("VALUE = 2\n", encoding="utf-8")
    git("add", "src/runtime.py")
    git("commit", "-qm", "runtime")
    runtime_head = git("rev-parse", "HEAD")
    assert (
        classify_git_range("push", evidence_head, runtime_head, tmp_path).runtime_relevant is True
    )
