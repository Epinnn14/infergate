# Model worker. Default image has the simulated backend only (small, starts in a second);
# build with --build-arg EXTRAS=hf for the Hugging Face backend (CPU torch, ~1 GB).
FROM python:3.12-slim
ARG EXTRAS=""
ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /app
COPY pyproject.toml ./
COPY infergate_worker ./infergate_worker
RUN if [ "$EXTRAS" = "hf" ]; then \
      pip install --index-url https://download.pytorch.org/whl/cpu torch && pip install ".[hf]"; \
    else pip install .; fi
RUN useradd --uid 10001 --create-home worker
USER 10001
ENV HF_HOME=/home/worker/.cache/huggingface
EXPOSE 9100
ENTRYPOINT ["infergate-worker"]
