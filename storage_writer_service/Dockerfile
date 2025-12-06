FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Create buffer and local files directories
RUN mkdir -p /tmp/storage_buffer /tmp/storage_files

CMD ["python", "storage_writer_service.py"]

