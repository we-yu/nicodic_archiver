FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir requests beautifulsoup4 lxml

COPY main.py .

ENTRYPOINT ["python", "main.py"]
