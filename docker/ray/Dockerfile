ARG RAY_BASE_IMAGE=ray-clickhouse-ray-base:2.58.0-py312-c3c9573c5c6b
FROM ${RAY_BASE_IMAGE}

WORKDIR /workspace/ray-clickhouse
COPY pyproject.toml README.md LICENSE ./
COPY src ./src
COPY tests ./tests

RUN python -m pip install --no-cache-dir "pytest>=8.3,<10" -e . \
    && python -m pip check
