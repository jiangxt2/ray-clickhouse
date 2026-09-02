"""Reproducible public-API comparison harness for Ray ClickHouse connectors.

The package initializer intentionally performs no eager imports. The lightweight fault proxy runs
in a controller environment without Ray, PyArrow, or connector dependencies; scenario modules load
their own runtime dependencies only in the official or external environments.
"""

__all__: list[str] = []
