# syntax=docker/dockerfile:1
FROM python:3.12-slim

WORKDIR /app/cloud

COPY geo_enrichment_service/requirements.txt .
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --retries 10 --timeout 120 \
      -i https://mirrors.aliyun.com/pypi/simple/ --trusted-host mirrors.aliyun.com \
      -r requirements.txt

COPY shared/ ./shared/
COPY geo_enrichment_service/ ./geo_enrichment_service/
