FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /app
COPY pyproject.toml ./
COPY infergate_registry ./infergate_registry
RUN pip install .
RUN useradd --uid 10001 registry
USER 10001
EXPOSE 8090
ENTRYPOINT ["uvicorn", "infergate_registry.app:app", "--host", "0.0.0.0", "--port", "8090", "--proxy-headers", "--no-access-log"]
