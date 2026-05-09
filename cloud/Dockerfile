FROM python:3.12-slim

# libgl1 + libglib2.0-0 required by opencv-python (cv2)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgl1 \
    libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app/cloud

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
