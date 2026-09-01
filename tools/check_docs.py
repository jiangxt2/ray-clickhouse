"""Validate the repository documentation contract without building Sphinx."""

from __future__ import annotations

import re
import sys
from collections.abc import Mapping
from pathlib import Path, PurePosixPath

REPOSITORY_ROOT = Path(__file__).resolve().parent.parent

REQUIRED_FILES = (
    "README.md",
    "CONTRIBUTING.md",
    "SECURITY.md",
    "doc/Makefile",
    "doc/source/conf.py",
    "doc/source/index.md",
    "doc/source/compatibility.md",
    "doc/source/architecture.md",
    "doc/source/security.md",
    "doc/source/troubleshooting.md",
    "doc/source/contributing.md",
    "doc/source/release-notes.md",
    "release-notes/v0.1.0.md",
)

README_LINKS = (
    "doc/source/index.md",
    "doc/source/compatibility.md",
    "doc/source/architecture.md",
    "doc/source/security.md",
    "doc/source/troubleshooting.md",
    "CONTRIBUTING.md",
    "SECURITY.md",
    "release-notes/v0.1.0.md",
)

TOCTREE_ENTRIES = (
    "compatibility",
    "architecture",
    "security",
    "troubleshooting",
    "contributing",
    "release-notes",
)

_HEADING = re.compile(r"^#{1,6}\s+(?P<title>.+?)\s*$", re.MULTILINE)
_NUMBERED_HEADING = re.compile(r"^\d+(?:\.\d+)*[.)]?\s+")
_MARKDOWN_LINK = re.compile(r"\[[^]]+\]\((?P<target>[^)]+)\)")
_LOCAL_PATH = re.compile(r"(?:/Users/[^\s`]+|~/GitHub(?:/[^\s`]*)?)")
_TEMPORARY_PUBLICATION_PHRASES = (
    "not yet published",
    "has not yet been published",
    "is being prepared as",
    "alpha release candidate",
    "has been published yet",
)


def repository_texts(root: Path = REPOSITORY_ROOT) -> dict[str, str]:
    """Read every required documentation file that exists."""
    return {
        path: (root / path).read_text(encoding="utf-8")
        for path in REQUIRED_FILES
        if (root / path).is_file()
    }


def _normalized_relative_target(source: str, target: str) -> str | None:
    target = target.strip().split(maxsplit=1)[0]
    if not target or target.startswith(("#", "http://", "https://", "mailto:")):
        return None
    without_fragment = target.split("#", 1)[0]
    if not without_fragment:
        return None
    combined = PurePosixPath(source).parent / without_fragment
    parts: list[str] = []
    for part in combined.parts:
        if part == "..":
            if parts:
                parts.pop()
        elif part not in {"", "."}:
            parts.append(part)
    return "/".join(parts)


def validate_document_texts(texts: Mapping[str, str]) -> list[str]:
    """Return every repository documentation policy violation."""
    errors: list[str] = []
    for path in REQUIRED_FILES:
        if path not in texts:
            errors.append(f"documentation is missing required file: {path}")

    for path, text in texts.items():
        for line_number, line in enumerate(text.splitlines(), start=1):
            if line.endswith((" ", "\t")):
                errors.append(f"{path}:{line_number} has trailing whitespace")
        if path.endswith(".md"):
            lowered = text.lower()
            for phrase in _TEMPORARY_PUBLICATION_PHRASES:
                if phrase in lowered:
                    errors.append(
                        f"{path} contains temporary publication state: {phrase!r}"
                    )
            for match in _HEADING.finditer(text):
                title = match.group("title")
                if _NUMBERED_HEADING.match(title):
                    errors.append(f"{path} has a numbered heading: {title!r}")
            local_path = _LOCAL_PATH.search(text)
            if local_path is not None:
                errors.append(
                    f"{path} exposes a machine-local path: {local_path.group(0)!r}"
                )
            for match in _MARKDOWN_LINK.finditer(text):
                target = _normalized_relative_target(path, match.group("target"))
                if target is not None and target not in texts:
                    errors.append(
                        f"{path} has a missing relative link target: {target}"
                    )

    readme = texts.get("README.md", "")
    for target in README_LINKS:
        if f"]({target})" not in readme:
            errors.append(f"README is missing documentation link: {target}")

    index = texts.get("doc/source/index.md", "")
    for entry in TOCTREE_ENTRIES:
        if f"\n{entry}\n" not in index:
            errors.append(f"documentation index is missing toctree entry: {entry}")

    makefile = texts.get("doc/Makefile", "")
    for target in ("html:", "spelling:", "linkcheck:"):
        if target not in makefile:
            errors.append(f"doc/Makefile is missing target: {target[:-1]}")
    for fragment in (
        "-W --keep-going -b html",
        "tools/check_docs.py",
        "codespell",
        "-W --keep-going -b linkcheck",
    ):
        if fragment not in makefile:
            errors.append(f"doc/Makefile is missing required command: {fragment!r}")

    security = texts.get("SECURITY.md", "")
    if "Do not disclose" not in security or "private" not in security:
        errors.append("SECURITY.md must require private vulnerability reporting")

    contributing = texts.get("CONTRIBUTING.md", "")
    for fragment in (
        "dry-run`, `testpypi`, `release-tag`, `pypi`, and `github-release",
        "`jiangxt2/ray-clickhouse`",
        "`release.yml`",
        "`testpypi`",
        "`pypi`",
        "pending Trusted Publishers",
        "gh attestation verify",
        "--source-digest <candidate-sha>",
    ):
        if fragment not in contributing:
            errors.append(f"CONTRIBUTING.md is missing release identity: {fragment!r}")

    return errors


def run_checks(root: Path = REPOSITORY_ROOT) -> list[str]:
    """Read repository documentation and return validation errors."""
    return validate_document_texts(repository_texts(root))


def main() -> int:
    errors = run_checks()
    if errors:
        for error in errors:
            print(f"ERROR: {error}", file=sys.stderr)
        return 1
    print("Documentation checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
