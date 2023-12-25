FROM python:3.11.3-slim

WORKDIR /app

RUN apt-get update && \
apt-get install -y default-jre-headless && \
rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY db2api.py .

COPY  db2jcc-db2jcc4.jar .

EXPOSE 8080

CMD [ "uvicorn", "db2api:app", "--host", "0.0.0.0", "--port", "8080"]