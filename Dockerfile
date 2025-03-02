FROM python:3.10

RUN apt-get update && apt-get install -y ffmpeg && pip install --no-cache-dir yt-dlp

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY video_pipeline.py .env ./

RUN mkdir -p /app/temp

# Will be overriden
ENV S3_BUCKET=""
ENV AWS_REGION="eu-central-1"
ENV KAFKA_BROKER="kafka:9092"
ENV KAFKA_TOPIC="video-processing"

CMD ["python", "video_pipeline.py"]