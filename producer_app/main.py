import json
import os.path
import shutil
import uuid

import av
import av.datasets
from kafka import KafkaProducer

# Define the Redpanda topic and Kafka producer
topic = "video_metadata"
producer = KafkaProducer(bootstrap_servers='localhost:19092',
                         key_serializer=str.encode,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Input video file for the demo is based on av.datasets
source_file = av.datasets.curated("pexels/time-lapse-video-of-night-sky-857195.mp4")


def get_video_metadata(file_path_in: str) -> dict:
    video_metadata = {}
    # Open the video file
    container = av.open(file_path_in)

    # Compose the metadata
    video_metadata["file_name"] = os.path.basename(container.name)
    video_metadata["title"] = container.metadata.get("title")
    video_metadata["duration"] = container.duration / av.time_base, "seconds"

    video_metadata["title"] = container.metadata.get("title")

    for stream in container.streams:
        if stream.type == 'video':
            video_metadata["video_resolution"] = str(stream.width) + " x " + str(stream.height)
            video_metadata["video_codec"] = stream.codec_context.codec.name

    return video_metadata


def store_file(file_path_in: str, file_path_out: str):
    # Copy the file
    shutil.copy(file_path_in, file_path_out)


metadata = get_video_metadata(source_file)

# Producer app's output file path
# This is the path in which the original video file will be stored by the Producer after the extraction of metadata
output_file = "producer_app/output/" + metadata.get("file_name")

# Store the video file
store_file(source_file, output_file)

# Generate message key which serves as Claim-check key
claim_check_key = str(uuid.uuid4())

# Send the video_metadata to the Redpanda topic
future = producer.send(topic, key=claim_check_key, value=metadata)
# Block until a single message is sent (or timeout in 15 seconds)
result = future.get(timeout=15)

print("Message sent, partition: ", result.partition, ", offset: ", result.offset)
