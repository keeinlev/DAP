FROM python:3.11-slim

WORKDIR /app

COPY ingestion_api/requirements.txt .
RUN pip install -r requirements.txt

COPY ingestion_api .
RUN mkdir /events
COPY configs/events/*.yaml /events

CMD ["python", "app.py"]