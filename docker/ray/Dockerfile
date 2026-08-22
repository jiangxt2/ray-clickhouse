FROM ray-cluster:2.55.1

WORKDIR /workspace/ray-clickhouse
COPY pyproject.toml README.md LICENSE ./
COPY src ./src
COPY tests ./tests

RUN python -m pip install --no-cache-dir \
        "clickhouse-connect[arrow]==1.5.0" \
        "pandas>=2,<3" \
        "pytest>=8,<10" \
        "pytest-asyncio>=0.23,<2" \
        hatchling \
    && python -m pip install --no-cache-dir --no-deps -e .
