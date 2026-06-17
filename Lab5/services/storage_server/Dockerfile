FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

WORKDIR /app

COPY services/storage_server/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY shared /app/shared
COPY services/storage_server /app/services/storage_server
COPY pipelines /app/pipelines

EXPOSE 8003

CMD ["uvicorn", "services.storage_server.app:app", "--host", "0.0.0.0", "--port", "8003"]
