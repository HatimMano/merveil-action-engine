FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED=1
ENV GCP_PROJECT_ID=merveil-data-warehouse

# Cloud Run Job : exécution unique, pas de serveur HTTP
CMD ["python", "main.py"]
