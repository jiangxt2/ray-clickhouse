from __future__ import annotations

from pathlib import Path

import pytest

from tools.check_docs import (
    REQUIRED_FILES,
    repository_texts,
    run_checks,
    validate_document_texts,
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]


def test_repository_documentation_contracts_match() -> None:
    assert run_checks() == []


def test_documentation_checker_rejects_missing_file() -> None:
    texts = repository_texts(REPOSITORY_ROOT)
    texts.pop("SECURITY.md")

    errors = validate_document_texts(texts)

    assert "documentation is missing required file: SECURITY.md" in errors


@pytest.mark.parametrize(
    ("path", "suffix", "expected"),
    (
        ("README.md", "\n## 1. Numbered\n", "numbered heading"),
        ("README.md", "\nLocal: /Users/example/project\n", "machine-local path"),
        ("README.md", "\nTrailing space \n", "trailing whitespace"),
        (
            "README.md",
            "\nVersion 0.1.0 is not yet published.\n",
            "temporary publication state",
        ),
        (
            "doc/source/security.md",
            "\n[Missing](missing.md)\n",
            "missing relative link target",
        ),
    ),
)
def test_documentation_checker_rejects_text_drift(
    path: str, suffix: str, expected: str
) -> None:
    texts = repository_texts(REPOSITORY_ROOT)
    texts[path] += suffix

    errors = validate_document_texts(texts)

    assert any(expected in error for error in errors)


def test_documentation_checker_requires_readme_navigation() -> None:
    texts = repository_texts(REPOSITORY_ROOT)
    texts["README.md"] = texts["README.md"].replace(
        "- [Security policy](SECURITY.md)\n", "", 1
    )

    errors = validate_document_texts(texts)

    assert "README is missing documentation link: SECURITY.md" in errors


def test_documentation_checker_requires_complete_toctree() -> None:
    texts = repository_texts(REPOSITORY_ROOT)
    texts["doc/source/index.md"] = texts["doc/source/index.md"].replace(
        "\ntroubleshooting\n", "\n", 1
    )

    errors = validate_document_texts(texts)

    assert "documentation index is missing toctree entry: troubleshooting" in errors


def test_required_document_list_is_unique() -> None:
    assert len(REQUIRED_FILES) == len(set(REQUIRED_FILES))


def test_documentation_checker_requires_exact_publisher_identity() -> None:
    texts = repository_texts(REPOSITORY_ROOT)
    texts["CONTRIBUTING.md"] = texts["CONTRIBUTING.md"].replace(
        "`jiangxt2/ray-clickhouse`", "`other/repository`"
    )

    errors = validate_document_texts(texts)

    assert any("release identity" in error for error in errors)
