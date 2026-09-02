# syntax=docker.m.daocloud.io/docker/dockerfile:1.7@sha256:a57df69d0ea827fb7266491f2813635de6f17269be881f696fbfdf2d83dda33e

ARG RAY_BASE_IMAGE=ray-clickhouse-comparison-ray-base:2.58.0-c3c9573c5c6b
FROM ${RAY_BASE_IMAGE}

USER root
ARG PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple
ENV PIP_INDEX_URL=${PIP_INDEX_URL} \
    PIP_CACHE_DIR=/var/cache/pip \
    PIP_DEFAULT_TIMEOUT=60 \
    PIP_RETRIES=8 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    RAY_USAGE_STATS_ENABLED=0

RUN python -m venv /opt/ray-clickhouse/official \
    && python -m venv /opt/ray-clickhouse/external \
    && python -m venv /opt/ray-clickhouse/controller

COPY comparison/official/env/official-requirements.txt /tmp/official-requirements.txt
COPY comparison/official/env/external-requirements.txt /tmp/external-requirements.txt
COPY --from=external-wheel / /tmp/external-wheel/
COPY --from=harness-wheel / /tmp/harness-wheel/

RUN --mount=type=cache,target=/var/cache/pip \
    /opt/ray-clickhouse/official/bin/python -m pip install \
        --report /opt/ray-clickhouse/official-install-report.json \
        -r /tmp/official-requirements.txt
RUN --mount=type=cache,target=/var/cache/pip \
    /opt/ray-clickhouse/external/bin/python -m pip install \
        --report /opt/ray-clickhouse/external-install-report.json \
        -r /tmp/external-requirements.txt
RUN /opt/ray-clickhouse/external/bin/python -m pip install --no-cache-dir \
        --no-deps /tmp/external-wheel/*.whl \
    && /opt/ray-clickhouse/controller/bin/python -m pip install --no-cache-dir \
        --no-deps /tmp/harness-wheel/*.whl \
    && /opt/ray-clickhouse/official/bin/python -m pip install --no-cache-dir \
        --no-deps /tmp/harness-wheel/*.whl \
    && /opt/ray-clickhouse/external/bin/python -m pip install --no-cache-dir \
        --no-deps /tmp/harness-wheel/*.whl \
    && /opt/ray-clickhouse/official/bin/python -m pip check \
    && /opt/ray-clickhouse/external/bin/python -m pip check \
    && /opt/ray-clickhouse/controller/bin/python -m pip check

WORKDIR /workspace/ray-clickhouse
COPY comparison/official/config comparison/official/config
COPY comparison/official/schema comparison/official/schema

RUN /opt/ray-clickhouse/official/bin/python -c \
        "from importlib.metadata import version; assert version('ray') == '2.58.0'; assert version('pyarrow') == '19.0.1'; assert version('clickhouse-connect') == '1.5.0'" \
    && /opt/ray-clickhouse/external/bin/python -c \
        "from importlib.metadata import version; assert version('ray') == '2.58.0'; assert version('ray-clickhouse') == '0.1.0'; assert version('pyarrow') == '19.0.1'; assert version('clickhouse-connect') == '1.5.0'"
