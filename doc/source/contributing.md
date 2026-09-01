# Contributing

The repository-root `CONTRIBUTING.md` is the authoritative guide for development setup, validation, integration-test requirements, commit format, and internal merge policy.

Project documentation uses MyST Markdown and Sphinx. Run the following before review:

```bash
uv sync --extra dev --group docs
uv run python tools/check_docs.py
make -C doc html
make -C doc spelling
```

Run `make -C doc linkcheck` separately so external link failures remain independently diagnosable.
