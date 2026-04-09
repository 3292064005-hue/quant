FROM python:3.11-slim
ENV PYTHONUNBUFFERED=1
WORKDIR /app
COPY pyproject.toml requirements.txt README.md ./
COPY a_share_quant ./a_share_quant
COPY configs ./configs
COPY sample_data ./sample_data
COPY scripts ./scripts
RUN pip install --no-cache-dir -r requirements.txt
CMD ["python", "-m", "a_share_quant", "--config", "configs/app.yaml"]
