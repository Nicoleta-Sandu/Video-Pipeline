# Video Processing Pipeline

A distributed system for downloading YouTube videos and automatically converting them to multiple formats using Apache Kafka, Docker, and AWS S3.

## Overview

This project implements a scalable video processing pipeline with two main components:

1. **Producer**: Downloads videos from YouTube and triggers processing tasks
2. **Consumer**: Processes videos into multiple formats (mp4, avi, webm, mkv)

The system uses Apache Kafka for message passing between components, Docker for containerization, and AWS S3 for storage.

## Architecture

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Producer  │    │    Kafka    │    │  Consumer   │
│             │───>│             │───>│             │
│ (Downloads) │    │  (Message   │    │ (Processes  │
│             │    │   Broker)   │    │   Videos)   │
└─────────────┘    └─────────────┘    └─────────────┘
       │                                     │
       │                                     │
       ▼                                     ▼
┌─────────────────────────────────────────────────┐
│                     AWS S3                       │
│                                                  │
│  (Stores original and processed video files)     │
└─────────────────────────────────────────────────┘
```

## Prerequisites

- Docker and Docker Compose
- AWS account with S3 bucket
- AWS credentials configured

## Configuration

Create a `.env` file in the project root with the following variables:

```
S3_BUCKET=your-s3-bucket-name
AWS_REGION=your-aws-region
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
```

## Setup and Installation

1. Clone the repository
2. Create the `.env` file with your configuration
3. Start the services:

```bash
docker-compose up -d
```

This will start:
- Zookeeper
- Kafka broker
- Producer service
- Consumer service

## How It Works

### Producer Service

The producer:
1. Downloads specified YouTube videos using `yt-dlp`
2. Uploads the downloaded videos to AWS S3
3. Produces Kafka events with S3 file location information

### Consumer Service

The consumer:
1. Listens for events from Kafka
2. Downloads the original video from S3
3. Converts the video to multiple formats (mp4, avi, webm, mkv) using `ffmpeg`
4. Uploads the converted videos back to S3

## Usage

The system starts processing automatically when launched. By default, it processes a predefined list of YouTube video IDs specified in the producer code.

### Adding More Videos

To process additional videos, modify the `ids` list in the `run_producer` function in `video_pipeline.py`:

```python
ids = ["bLVKTbxPmcg", "SR__amDl1c8", "your-new-video-id"]
```

Then restart the producer:

```bash
docker-compose restart producer
```

### Viewing Logs

```bash
# View all logs
docker-compose logs

# View logs for a specific service
docker-compose logs producer
docker-compose logs consumer
```

## File Structure

- `docker-compose.yaml`: Defines the multi-container application
- `Dockerfile`: Instructions for building the application container
- `requirements.txt`: Python dependencies
- `video_pipeline.py`: Main application code
- `.env`: Environment variables (not included, must be created)

## Troubleshooting

### Common Issues

1. **Kafka Connection Issues**:
   - Check if Kafka and Zookeeper containers are running
   - Verify network configuration in docker-compose.yaml

2. **S3 Access Issues**:
   - Ensure AWS credentials are correct
   - Verify S3 bucket exists and is accessible

3. **Video Processing Failures**:
   - Check consumer logs for ffmpeg errors
   - Ensure ffmpeg is installed correctly in the container

## Scaling

The system can be scaled horizontally by adding more consumer instances:

```bash
docker-compose up -d --scale consumer=3
```

## Notes

- The system is configured to download videos at a maximum resolution of 720p
- Videos are stored in the S3 bucket under the following paths:
  - Original videos: `videos/{filename}`
  - Converted videos: `encoded/{video_id}/{video_id}.{format}`