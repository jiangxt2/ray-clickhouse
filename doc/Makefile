SPHINXBUILD ?= ../.venv/bin/sphinx-build
CODESPELL ?= .venv/bin/codespell
SOURCEDIR = source
BUILDDIR = build

.PHONY: html spelling linkcheck

html:
	$(SPHINXBUILD) -W --keep-going -b html $(SOURCEDIR) $(BUILDDIR)/html

spelling:
	cd .. && .venv/bin/python tools/check_docs.py
	cd .. && $(CODESPELL) README.md CONTRIBUTING.md SECURITY.md doc/source release-notes

linkcheck:
	$(SPHINXBUILD) -W --keep-going -b linkcheck $(SOURCEDIR) $(BUILDDIR)/linkcheck
