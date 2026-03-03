FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY nicodic_archiver/ ./nicodic_archiver/

VOLUME ["/app/data"]

ENTRYPOINT ["python", "-m", "nicodic_archiver.cli"]
