import os
import sys
import json
import time
import tempfile
import subprocess
import boto3
import ffmpeg
from pathlib import Path
from kafka import KafkaProducer, KafkaConsumer
from dotenv import load_dotenv

load_dotenv()

# Configuration

# S3 Config
S3_BUCKET = os.environ.get('S3_BUCKET')
AWS_REGION = os.environ.get('AWS_REGION')

# Kafka Config
KAFKA_BROKERS = [os.environ.get('KAFKA_BROKER', 'kafka:9092')]
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'video-processing')

# Video processing config
OUTPUT_FORMATS = ["mp4", "avi", "webm", "mkv"]

# Initialize S3 client (using boto3 library)
s3_client = boto3.client (
    's3',
    region_name=AWS_REGION
)

# S3 Interface
def upload_to_s3(local_file_path, s3_key):
    try:
        s3_client.upload_file(local_file_path, S3_BUCKET, s3_key)
        print(f"Uploaded {local_file_path} to S3 bucket {S3_BUCKET} with key {s3_key}")
        return f"s3://{S3_BUCKET}/{s3_key}"
    except Exception as e:
        print(f"Error uploading to S3: {e}")
        raise

def download_from_s3(s3_key, local_path):
    try:
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        s3_client.download_file(S3_BUCKET, s3_key, local_path)
        print(f"Downloaded s3://{S3_BUCKET}/{s3_key} to {local_path}")
        return local_path
    except Exception as e:
        print(f"Error downloading from S3: {e}")
        raise

# YT Download Func
def download_youtube_video(video_url, output_path):
    # get filename that yt-dlp will use
    get_filename_cmd = [
        "yt-dlp", "--get-filename",
        "-f", "bv*[height<=720][ext=mp4]+ba[ext=m4a]/b[height<=720]/b",
        "-o", f"{output_path}.%(ext)s",
        video_url
    ]

    try:
        output_filename = subprocess.check_output(get_filename_cmd, text=True).strip()

        # download the video
        download_cmd = [
            "yt-dlp",
            "-f", "bv*[height<=720][ext=mp4]+ba[ext=m4a]/b[height<=720]/b",
            "-o", f"{output_path}.%(ext)s",
            video_url
        ]

        print(f"Executing: {' '.join(download_cmd)}")
        subprocess.run(download_cmd, check=True, text=True)

        return output_filename
    except subprocess.CalledProcessError as e:
        print(f"Error downloading video: {e}")
        raise

# Kafka Producer Func
def produce_event(s3_key):
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKERS,
            value_serializer=lambda v:json.dumps(v).encode('utf-8')
        )

        event_data = {
            "s3_key": s3_key,
            "bucket": S3_BUCKET,
            "timestamp": time.time()
        }

        producer.send(KAFKA_TOPIC, event_data)
        producer.flush()
        print(f"Produces Kafka event: {event_data}")
    except Exception as e:
        print(f"Error producing Kafka event: {e}")
        raise

# Video Encoding Func
def encode_video(input_path, format):
    input_basename = os.path.splitext(os.path.basename(input_path))[0]
    temp_dir = tempfile.mkdtemp()

    output_path = os.path.join(temp_dir, f"{input_basename}.{format}")
    print(f"Started encoding to {format}: {input_path}")

    try:
        (
            ffmpeg
            .input(input_path)
            .output(output_path)
            .run(capture_stdout=True, capture_stderr=True)
        )
        print(f"Finished encoding to {format}: {output_path}")
        return output_path
    except ffmpeg.Error as e:
        print(f"Error encoding to {format}: {e.stderr.decode()}")
        raise

# Producer Logic
def run_producer():
    # producer workflow: downloads videos, uploads to S3, produces events
    if not S3_BUCKET:
        print("Error: S3_BUCKET environment variable must be set")
        sys.exit(1)

    ids = ["bLVKTbxPmcg", "SR__amDl1c8"]

    for video_id in ids:
        try:
            # temp dir for downloads
            with tempfile.TemporaryDirectory() as temp_dir:
                output_filename = f"downloaded_video_{video_id}"
                local_output_path = os.path.join(temp_dir, output_filename)
                video_url = f"https://www.youtube.com/watch?v={video_id}"

                print(f"Downloading video from YouTube: {video_url}")
                local_output_path = download_youtube_video(video_url, local_output_path)
                print(f"Downloaded video to: {local_output_path}")

                # upload the video to S3
                s3_key = f"videos/{os.path.basename(local_output_path)}"
                upload_to_s3(local_output_path, s3_key)
                print(f"Stored video with key: {s3_key}")

                # produce Kafka event
                produce_event(s3_key)

                print(f"Successfully processed video {video_id}")

        except Exception as e:
            print(f"Error processing video {video_id}: {e}")

# Consumer logic
def process_video(s3_key, bucket):
    # downloads from S3, encodes it (4 formats), uploads encoded versions
    try:
        # create temp dir for processing
        with tempfile.TemporaryDirectory() as temp_dir:
            local_video_path = os.path.join(temp_dir, os.path.basename(s3_key))

            # download from S3
            download_from_s3(s3_key, local_video_path)

            # encode into each format
            for output_format in OUTPUT_FORMATS:
                # skip if the video is already in this format
                if local_video_path.lower().endswith(f".{output_format.lower()}"):
                    print(f"Skipping encoding to {output_format}: video is already in this format")
                    continue

                try:
                    encoded_path = encode_video(local_video_path, output_format)
                    
                    # Upload encoded file to S3
                    input_basename = os.path.splitext(os.path.basename(s3_key))[0]
                    encoded_s3_key = f"encoded/{input_basename}/{input_basename}.{output_format}"
                    upload_to_s3(encoded_path, encoded_s3_key)
                    print(f"Uploaded encoded video: {encoded_s3_key}")
                    
                except Exception as e:
                    print(f"Error encoding to {output_format}: {e}")
            
            print(f"Successfully processed video {s3_key}")
            
    except Exception as e:
        print(f"Error processing video {s3_key}: {e}")

def run_consumer():
    # consumer workflow: listens for events and processes videos
    if not S3_BUCKET:
        print("Error: S3_BUCKET environment variable must be set")
        sys.exit(1)

    print("Starting consumer, waiting for Kafka connections...")
    time.sleep(30)

    try:
        print(f"Attempting to connect to Kafka brokers: {KAFKA_BROKERS}")
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BROKERS,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            auto_offset_reset='earliest',
            group_id='video-processor'
        )

        print(f"Consumer connected, listening for events on topic: {KAFKA_TOPIC}")

        for message in consumer:
            try:
                event_data = message.value
                s3_key = event_data.get('s3_key')
                bucket = event_data.get('bucket', S3_BUCKET)

                if s3_key:
                    print(f"Received event for video: {s3_key} in bucket: {bucket}")
                    process_video(s3_key, bucket)
                else:
                    print(f"Received event without s3_key: {event_data}")
            
            except Exception as e:
                print(f"Error processing message: {e}")

    except Exception as e:
        print(f"Consumer error: {e}")
        print(f"Error details: {str(e)}")
        import traceback
        traceback.print_exc()

# Main Entry Point
def main():
    if len(sys.argv) < 2:
        print("Please specify mode: producer or consumer")
        sys.exit(1)

    mode = sys.argv[1]

    if mode == "producer":
        run_producer()
    elif mode == "consumer":
        run_consumer()
    else:
        print('Unknown mode. Use "producer" or "consumer".')
        sys.exit(1)

if __name__ == "__main__":
    main()
