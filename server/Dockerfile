FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

# Diretório de armazenamento temporário dos arquivos
RUN mkdir -p /app/storage
ENV STORAGE_DIR=/app/storage
ENV MAX_FILE_SIZE_MB=200
ENV EXPIRATION_HOURS=6
ENV DELETE_AFTER_DOWNLOAD=true

EXPOSE 8000

CMD ["python", "main.py"]
