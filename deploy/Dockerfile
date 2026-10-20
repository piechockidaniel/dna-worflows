# DnA Workflows image: Python runtime + the built Studio. Build from the repository root:
#   docker build -f deploy/Dockerfile -t DnA Workflows .

FROM node:22-slim AS studio
WORKDIR /studio
COPY studio/package.json studio/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY studio/ ./
RUN npm run build

FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install ".[rabbitmq,servicebus,otel,prometheus,api,forecasting,mcp,postgres]"
COPY --from=studio /studio/dist ./studio
COPY examples ./examples
RUN useradd --create-home DnA Workflows && mkdir -p /data && chown DnA Workflows /data
USER DnA Workflows
WORKDIR /app/examples/order_integration
EXPOSE 8080
HEALTHCHECK CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8080/health')"
CMD ["python", "-m", "DnA Workflows", "run", "--config", "shell.production.yaml"]
