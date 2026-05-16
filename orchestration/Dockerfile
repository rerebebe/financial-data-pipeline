FROM python:3.12-slim

WORKDIR /app
COPY producer.py .
RUN pip install --no-cache-dir \
    requests \
    python-dotenv \
    google-cloud-pubsub \
    pytz

CMD ["python", "producer.py"]
# python producer.py

