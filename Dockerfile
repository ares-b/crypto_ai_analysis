FROM ghcr.io/astral-sh/uv@sha256:e5b65587bce7de595f299855d7385fe7fca39b8a74baa261ba1b7147afa78e58 AS builder

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY README.md ./
COPY src/ ./src/
COPY workspace.yaml ./
RUN uv sync --frozen --no-dev


FROM python@sha256:d764629ce0ddd8c71fd371e9901efb324a95789d2315a47db7e4d27e78f1b0e9

COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/src   /app/src
COPY --from=builder /app/workspace.yaml /app/workspace.yaml

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONPATH=/app/src \
    DAGSTER_HOME=/dagster_home \
    PYTHONUNBUFFERED=1

RUN mkdir -p "${DAGSTER_HOME}" \
    && useradd -r -u 1001 -g root dagster \
    && chown -R dagster:root /app "${DAGSTER_HOME}"

USER dagster

EXPOSE 3030

CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "3030", "-m", "orchestration"]
