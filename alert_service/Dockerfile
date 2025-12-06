FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Create log directory
RUN mkdir -p /var/log/alerts

CMD ["python", "alert_service.py"]

