FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app
ENV ULTRALYTICS_CONFIG_DIR=/tmp/ultralytics

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends libgl1 libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

COPY services/detection_server/requirements.txt /app/requirements.txt

# Install CPU-only PyTorch first. Otherwise pip may pull large CUDA wheels.
RUN pip install --no-cache-dir \
    --index-url https://download.pytorch.org/whl/cpu \
    torch==2.2.2+cpu \
    torchvision==0.17.2+cpu

RUN pip install --no-cache-dir -r /app/requirements.txt

COPY shared /app/shared
COPY services/detection_server /app/services/detection_server

CMD ["python", "-m", "services.detection_server.worker"]

