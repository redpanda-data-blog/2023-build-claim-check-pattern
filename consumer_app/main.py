import json
import os

import av
from kafka import KafkaConsumer

# Define the Redpanda topic and Kafka producer
topic = "video_metadata"
consumer = KafkaConsumer(topic,
                         bootstrap_servers='localhost:19092',
                         value_deserializer=lambda v: json.loads(v.decode('utf-8')))
output_file_base_path = "consumer_app/output/"


def get_producer_output_directory() -> str:
    # Get the directory of the current script
    current_file_directory = os.path.dirname(os.path.abspath(__file__))

    # Relative path to your video file
    source_file_base_path = os.path.join(current_file_directory, "../producer_app/output/")
    return source_file_base_path


# Define a function to convert an MP4 video file to MKV format
def convert_mp4_to_mkv(mp4_file_in: str):
    # Print a message indicating the start of the conversion process
    print("Converting the video " + mp4_file_in + " from mp4 to mkv format")

    # Open the input video file
    input_ = av.open(mp4_file_in)
    # Open the output video file in write mode
    output = av.open(output_file_base_path + "remuxed.mkv", "w")

    # Get the first video stream from the input file
    in_stream = input_.streams.video[0]
    # Add a new stream to the output file, using the input stream as a template
    out_stream = output.add_stream(template=in_stream)

    # Loop over all packets in the input stream
    for packet in input_.demux(in_stream):

        # If the packet's decoding timestamp (DTS) is None, skip this packet
        if packet.dts is None:
            continue

        # Set the packet's stream to the output stream
        packet.stream = out_stream

        # Multiplex ("mux") the packet to the output stream
        output.mux(packet)

    # Close the input file
    input_.close()
    # Close the output file
    output.close()


# Consume the message from the topic
for message in consumer:
    # Get the metadata information (such as file_name) from the message
    file_name = message.value.get("file_name")
    # Construct the MP4 file path
    mp4_file = get_producer_output_directory() + file_name
    # Convert the MP4 video format file to MKV format and save it in the output_file_base_path location
    convert_mp4_to_mkv(mp4_file)
    print("Converted the mp4 video: " + file_name + " to mkv format!")
