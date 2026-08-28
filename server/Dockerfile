FROM python:3.12-slim

WORKDIR /app

COPY requirements_server.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

RUN mkdir -p /app/storage
ENV STORAGE_DIR=/app/storage
ENV MAX_FILE_SIZE_MB=200
ENV EXPIRATION_HOURS=6
ENV DELETE_AFTER_DOWNLOAD=true

EXPOSE 8000

CMD ["python", "main.py"]
