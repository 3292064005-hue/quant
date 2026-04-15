FROM python:3.11-slim
ENV PYTHONUNBUFFERED=1
ARG DIST_PROFILE=core
WORKDIR /app
COPY pyproject.toml requirements.txt requirements-core.txt requirements-workstation.txt requirements-production.txt README.md ./
COPY a_share_quant ./a_share_quant
COPY configs ./configs
COPY sample_data ./sample_data
COPY scripts ./scripts
RUN case "$DIST_PROFILE" in \
      core) pip install --no-cache-dir -r requirements-core.txt ;; \
      workstation) pip install --no-cache-dir -r requirements-workstation.txt ;; \
      production) pip install --no-cache-dir -r requirements-production.txt ;; \
      *) echo "unsupported DIST_PROFILE=$DIST_PROFILE" && exit 1 ;; \
    esac
CMD ["python", "-m", "a_share_quant", "--config", "configs/app.yaml"]
