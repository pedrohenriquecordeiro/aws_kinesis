import boto3
import json
from dotenv import load_dotenv
import os

# Load environment variables from the .env file
load_dotenv()

def send_data_to_firehose(stream_name, data):
    # Fetch AWS credentials from environment variables
    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    aws_region = os.getenv('AWS_REGION')

    # Initialize a Firehose client with credentials from .env file
    firehose_client = boto3.client(
        'firehose',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=aws_region
    )

    # Prepare the data to send
    json_data = json.dumps(data).encode('utf-8')

    try:
        # Send data to the Firehose stream
        response = firehose_client.put_record(
            DeliveryStreamName=stream_name,
            Record={
                'Data': json_data
            }
        )

        # Check response status
        print(f"Record sent: {response}")
        return response

    except Exception as e:
        print(f"Error sending record to Firehose: {e}")
        return None


if __name__ == "__main__":
    # Replace with your actual Firehose stream name
    firehose_stream_name = "your-firehose-stream-name"

    # Example data to send
    data_record = {
        "user_id": 123,
        "action": "click",
        "timestamp": "2024-10-21T10:00:00Z",
        "details": {
            "page": "home",
            "button_id": "signup"
        }
    }

    # Send the data to the Firehose stream
    send_data_to_firehose(firehose_stream_name, data_record)
