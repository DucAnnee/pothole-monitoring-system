# syntax=docker/dockerfile:1
FROM python:3.12-slim

# libgl1 + libglib2.0-0 required by opencv (cv2)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgl1 \
    libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app/cloud

COPY bev_surface_service/requirements.txt .
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --retries 10 --timeout 120 \
      -i https://mirrors.aliyun.com/pypi/simple/ --trusted-host mirrors.aliyun.com \
      -r requirements.txt

COPY shared/ ./shared/
COPY .conf/ ./.conf/
COPY bev_surface_service/ ./bev_surface_service/
