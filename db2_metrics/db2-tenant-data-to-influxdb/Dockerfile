FROM python:3.11.3-slim

WORKDIR .

RUN apt-get update && \
apt-get install -y default-jre-headless && \
rm -rf /var/lib/apt/lists/*

COPY . .

RUN pip install -r requirements.txt

EXPOSE 8080

CMD ["python3" , "main.py"]


