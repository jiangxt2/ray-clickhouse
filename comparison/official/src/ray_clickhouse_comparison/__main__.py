"""Command-line entry point for comparison validation and execution."""

from __future__ import annotations

import argparse
import json
import re
from collections.abc import Mapping
from pathlib import Path

from ray_clickhouse_comparison.config import load_reference, load_scenarios, scenario_digest
from ray_clickhouse_comparison.evidence import (
    atomic_write_json,
    build_context_manifest,
    collect_case_evidence,
    load_schema,
    pip_report_identity,
    read_jsonl,
    sanitize_tree,
    sanitize_value,
    sensitive_findings,
    validate_complete_tree,
    validate_document,
    verify_wheel_sources,
    wheel_identity,
)
from ray_clickhouse_comparison.fixtures import make_fixture, write_fixture
from ray_clickhouse_comparison.metrics import (
    summarize_docker_stats,
    summarize_process_samples,
    summarize_ray_metric_samples,
)
from ray_clickhouse_comparison.runner import (
    _load_scenario,
    cleanup_permission_fixture,
    execute,
    prepare_scenario,
    validate_smoke_result,
    warmup_scenario,
)
from ray_clickhouse_comparison.summary import summarize_results


def _validate(args: argparse.Namespace) -> None:
    reference = load_reference(args.reference)
    scenarios = load_scenarios(args.scenarios)
    if any(
        scenario.resource_metrics_required
        and scenario.repetitions != reference.measured_repetitions
        for scenario in scenarios
    ):
        raise ValueError("resource scenarios must use the reference measured repetition count")
    load_schema(args.manifest_schema)
    load_schema(args.result_schema)
    print(
        json.dumps(
            {
                "runtime_base_commit": reference.runtime_base_commit,
                "scenario_count": len(scenarios),
                "scenario_digest": scenario_digest(scenarios),
            },
            sort_keys=True,
        )
    )


def _fixture(args: argparse.Namespace) -> None:
    identity = write_fixture(
        args.output,
        make_fixture(args.rows, seed=args.seed, payload_bytes=args.payload_bytes),
    )
    print(json.dumps(identity, sort_keys=True))


def _summarize(args: argparse.Namespace) -> None:
    rows = read_jsonl(args.results)
    schema = load_schema(args.result_schema)
    for row in rows:
        validate_document(row, schema)
    atomic_write_json(args.output, summarize_results(rows))


def _sanitize(args: argparse.Namespace) -> None:
    value = json.loads(args.input.read_text(encoding="utf-8"))
    sanitized, classes = sanitize_value(value)
    payload = json.dumps(sanitized, sort_keys=True, indent=2) + "\n"
    findings = sensitive_findings(payload)
    if findings:
        raise ValueError(f"sanitized output still contains sensitive classes: {findings}")
    args.output.write_text(payload, encoding="utf-8")
    atomic_write_json(
        args.report,
        {"schema_version": 1, "removed_classes": list(classes), "remaining_findings": []},
    )


def _sanitize_tree(args: argparse.Namespace) -> None:
    if args.require_complete:
        if args.mode is None or args.scenarios is None:
            raise ValueError("complete sanitization requires --mode and --scenarios")
        validate_complete_tree(
            args.input,
            mode=args.mode,
            scenarios_path=args.scenarios,
        )
    report = sanitize_tree(args.input, args.output, require_complete=args.require_complete)
    print(json.dumps(report, sort_keys=True))


def _manifest(args: argparse.Namespace) -> None:
    reference = load_reference(args.reference)
    scenarios = load_scenarios(args.scenarios)
    official = pip_report_identity(args.official_report, "ray")
    harness = wheel_identity(args.harness_wheel, "ray-clickhouse-official-comparison")
    harness["commit"] = args.harness_commit
    external = wheel_identity(args.external_wheel, "ray-clickhouse")
    external["commit"] = reference.runtime_base_commit
    if official["version"] != reference.ray_version:
        raise ValueError("official Ray wheel version differs from the reference contract")
    if args.mode != "smoke" and args.harness_git_state != "clean":
        raise ValueError("remote comparison modes require a clean harness commit")
    sha_pattern = re.compile(r"^[0-9a-f]{40}$")
    if not sha_pattern.fullmatch(args.harness_commit):
        raise ValueError("harness commit must be a full lowercase commit SHA")
    if not sha_pattern.fullmatch(args.candidate_sha):
        raise ValueError("candidate SHA must be a full lowercase commit SHA")
    if not sha_pattern.fullmatch(args.workflow_sha):
        raise ValueError("workflow SHA must be a full lowercase commit SHA")
    if args.candidate_sha != args.harness_commit:
        raise ValueError("candidate SHA must equal the checked-out harness commit")
    digests = (
        args.controller_lock_sha256,
        args.official_requirements_sha256,
        args.external_requirements_sha256,
        args.result_schema_sha256,
    )
    if any(
        len(value) != 64 or any(character not in "0123456789abcdef" for character in value)
        for value in digests
    ):
        raise ValueError("manifest SHA-256 inputs must be lowercase hexadecimal digests")
    document = {
        "schema_version": 1,
        "run_id": args.run_id,
        "harness": harness,
        "official": official,
        "external": external,
        "provenance": {
            "candidate_sha": args.candidate_sha,
            "harness_commit": args.harness_commit,
            "workflow_sha": args.workflow_sha,
        },
        "environment": {
            "python_version": reference.python_version,
            "ray_version": reference.ray_version,
            "clickhouse_version": reference.clickhouse_version,
            "clickhouse_image": reference.clickhouse_image,
            "ray_base_image": reference.ray_base_image,
            "runtime_image_id": args.runtime_image_id,
            "mode": args.mode,
            "harness_git_state": args.harness_git_state,
            "controller_lock_sha256": args.controller_lock_sha256,
            "official_requirements_sha256": args.official_requirements_sha256,
            "external_requirements_sha256": args.external_requirements_sha256,
            "result_schema_version": 1,
            "result_schema_sha256": args.result_schema_sha256,
        },
        "scenario_digest": scenario_digest(scenarios),
        "artifacts": [],
    }
    schema = load_schema(args.manifest_schema)
    validate_document(document, schema)
    atomic_write_json(args.output, document)


def _validate_result(args: argparse.Namespace) -> None:
    document = json.loads(args.result.read_text(encoding="utf-8"))
    if not isinstance(document, dict):
        raise ValueError("result must be an object")
    validate_document(document, load_schema(args.result_schema))
    scenario = _load_scenario(args.scenarios, args.scenario)
    validate_smoke_result(document, scenario, args.mode)


def _collect(args: argparse.Namespace) -> None:
    result_count, query_count = collect_case_evidence(
        args.input,
        args.results,
        args.queries,
        load_schema(args.result_schema),
    )
    print(json.dumps({"result_count": result_count, "query_count": query_count}, sort_keys=True))


def _resource_summary(args: argparse.Namespace) -> None:
    summary: dict[str, int | float | str | bool] = {}
    complete = True

    def record(values: Mapping[str, int | float | str | bool]) -> None:
        nonlocal complete
        missing_baseline_locations = values.get(
            "ray_object_store_baseline_missing_location_count", 0
        )
        missing_measured_locations = values.get(
            "ray_object_store_measured_missing_location_count", 0
        )
        if missing_baseline_locations or missing_measured_locations:
            complete = False
            if args.require_complete:
                raise ValueError("Ray Object Store evidence is missing locations")
        summary.update(values)

    collectors = (
        (summarize_docker_stats, args.docker_stats),
        (summarize_process_samples, args.process_samples),
    )
    for collector, path in collectors:
        try:
            record(collector(path))
        except (FileNotFoundError, ValueError):
            if args.require_complete:
                raise
            complete = False
    try:
        record(
            summarize_ray_metric_samples(
                args.ray_metrics,
                allow_incomplete=not args.require_complete,
            )
        )
    except (FileNotFoundError, ValueError):
        if args.require_complete:
            raise
        complete = False
    summary["telemetry_complete"] = complete
    atomic_write_json(args.output, summary)


def _verify_wheel(args: argparse.Namespace) -> None:
    verify_wheel_sources(args.wheel, args.source, args.package)


def _context_manifest(args: argparse.Namespace) -> None:
    print(json.dumps(build_context_manifest(args.root, args.output), sort_keys=True))


def _prepare(args: argparse.Namespace) -> None:
    prepare_scenario(
        side=args.side,
        scenario=_load_scenario(args.scenarios, args.scenario),
        mode=args.mode,
        expected_output=args.expected_identity,
    )


def _cleanup_permission(args: argparse.Namespace) -> None:
    del args
    cleanup_permission_fixture()


def _warmup(args: argparse.Namespace) -> None:
    warmup_scenario(
        side=args.side,
        scenario=_load_scenario(args.scenarios, args.scenario),
        reference=load_reference(args.reference),
        run_id=args.run_id,
    )


def _run(args: argparse.Namespace) -> None:
    execute(
        side=args.side,
        scenario=_load_scenario(args.scenarios, args.scenario),
        reference=load_reference(args.reference),
        run_id=args.run_id,
        repetition=args.repetition,
        output=args.output,
        result_schema=args.result_schema,
        control_dir=args.control_dir,
        expected_identity_path=args.expected_identity,
    )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)

    validate = commands.add_parser("validate")
    validate.add_argument("--reference", type=Path, required=True)
    validate.add_argument("--scenarios", type=Path, required=True)
    validate.add_argument("--manifest-schema", type=Path, required=True)
    validate.add_argument("--result-schema", type=Path, required=True)
    validate.set_defaults(handler=_validate)

    fixture = commands.add_parser("fixture")
    fixture.add_argument("--output", type=Path, required=True)
    fixture.add_argument("--rows", type=int, default=256)
    fixture.add_argument("--seed", type=int, default=20260901)
    fixture.add_argument("--payload-bytes", type=int, default=32)
    fixture.set_defaults(handler=_fixture)

    summarize = commands.add_parser("summarize")
    summarize.add_argument("--results", type=Path, required=True)
    summarize.add_argument("--result-schema", type=Path, required=True)
    summarize.add_argument("--output", type=Path, required=True)
    summarize.set_defaults(handler=_summarize)

    sanitize = commands.add_parser("sanitize")
    sanitize.add_argument("--input", type=Path, required=True)
    sanitize.add_argument("--output", type=Path, required=True)
    sanitize.add_argument("--report", type=Path, required=True)
    sanitize.set_defaults(handler=_sanitize)

    sanitize_tree_parser = commands.add_parser("sanitize-tree")
    sanitize_tree_parser.add_argument("--input", type=Path, required=True)
    sanitize_tree_parser.add_argument("--output", type=Path, required=True)
    sanitize_tree_parser.add_argument("--mode", choices=("smoke", "dry-run", "formal"))
    sanitize_tree_parser.add_argument("--scenarios", type=Path)
    sanitize_tree_parser.add_argument("--require-complete", action="store_true")
    sanitize_tree_parser.set_defaults(handler=_sanitize_tree)

    manifest = commands.add_parser("manifest")
    manifest.add_argument("--reference", type=Path, required=True)
    manifest.add_argument("--scenarios", type=Path, required=True)
    manifest.add_argument("--manifest-schema", type=Path, required=True)
    manifest.add_argument("--official-report", type=Path, required=True)
    manifest.add_argument("--run-id", required=True)
    manifest.add_argument("--mode", choices=("smoke", "dry-run", "formal"), required=True)
    manifest.add_argument("--harness-commit", required=True)
    manifest.add_argument("--candidate-sha", required=True)
    manifest.add_argument("--workflow-sha", required=True)
    manifest.add_argument("--harness-git-state", choices=("clean", "dirty"), required=True)
    manifest.add_argument("--harness-wheel", type=Path, required=True)
    manifest.add_argument("--external-wheel", type=Path, required=True)
    manifest.add_argument("--runtime-image-id", required=True)
    manifest.add_argument("--controller-lock-sha256", required=True)
    manifest.add_argument("--official-requirements-sha256", required=True)
    manifest.add_argument("--external-requirements-sha256", required=True)
    manifest.add_argument("--result-schema-sha256", required=True)
    manifest.add_argument("--output", type=Path, required=True)
    manifest.set_defaults(handler=_manifest)

    validate_result = commands.add_parser("validate-result")
    validate_result.add_argument("--result", type=Path, required=True)
    validate_result.add_argument("--result-schema", type=Path, required=True)
    validate_result.add_argument("--scenario", required=True)
    validate_result.add_argument("--scenarios", type=Path, required=True)
    validate_result.add_argument("--mode", choices=("smoke", "dry-run", "formal"), required=True)
    validate_result.set_defaults(handler=_validate_result)

    collect = commands.add_parser("collect")
    collect.add_argument("--input", type=Path, required=True)
    collect.add_argument("--results", type=Path, required=True)
    collect.add_argument("--queries", type=Path, required=True)
    collect.add_argument("--result-schema", type=Path, required=True)
    collect.set_defaults(handler=_collect)

    resource_summary = commands.add_parser("resource-summary")
    resource_summary.add_argument("--docker-stats", type=Path, required=True)
    resource_summary.add_argument("--process-samples", type=Path, required=True)
    resource_summary.add_argument("--ray-metrics", type=Path, required=True)
    resource_summary.add_argument("--require-complete", action="store_true")
    resource_summary.add_argument("--output", type=Path, required=True)
    resource_summary.set_defaults(handler=_resource_summary)

    verify_wheel = commands.add_parser("verify-wheel")
    verify_wheel.add_argument("--wheel", type=Path, required=True)
    verify_wheel.add_argument("--source", type=Path, required=True)
    verify_wheel.add_argument("--package", required=True)
    verify_wheel.set_defaults(handler=_verify_wheel)

    context_manifest = commands.add_parser("context-manifest")
    context_manifest.add_argument("--root", type=Path, required=True)
    context_manifest.add_argument("--output", type=Path, required=True)
    context_manifest.set_defaults(handler=_context_manifest)

    prepare = commands.add_parser("prepare")
    prepare.add_argument("--side", choices=("official", "external"), required=True)
    prepare.add_argument("--scenario", required=True)
    prepare.add_argument("--scenarios", type=Path, required=True)
    prepare.add_argument("--mode", choices=("smoke", "dry-run", "formal"), required=True)
    prepare.add_argument("--expected-identity", type=Path, required=True)
    prepare.set_defaults(handler=_prepare)

    cleanup_permission = commands.add_parser("cleanup-permission")
    cleanup_permission.set_defaults(handler=_cleanup_permission)

    warmup = commands.add_parser("warmup")
    warmup.add_argument("--side", choices=("official", "external"), required=True)
    warmup.add_argument("--scenario", required=True)
    warmup.add_argument("--reference", type=Path, required=True)
    warmup.add_argument("--scenarios", type=Path, required=True)
    warmup.add_argument("--run-id", required=True)
    warmup.set_defaults(handler=_warmup)

    run = commands.add_parser("run")
    run.add_argument("--side", choices=("official", "external"), required=True)
    run.add_argument("--scenario", required=True)
    run.add_argument("--reference", type=Path, required=True)
    run.add_argument("--scenarios", type=Path, required=True)
    run.add_argument("--run-id", required=True)
    run.add_argument("--repetition", type=int, default=0)
    run.add_argument("--output", type=Path, required=True)
    run.add_argument("--result-schema", type=Path, required=True)
    run.add_argument("--control-dir", type=Path, required=True)
    run.add_argument("--expected-identity", type=Path, required=True)
    run.set_defaults(handler=_run)

    args = parser.parse_args(argv)
    args.handler(args)


if __name__ == "__main__":
    main()
