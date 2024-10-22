import boto3 , csv , random , time , os , logging , string , json
from dotenv import load_dotenv
from faker import Faker

# Load environment variables from the .env file
load_dotenv()
fake = Faker()

def generate_random_numeric_string(length):
    numbers = string.digits  # '0123456789'
    random_numbers = ''.join(random.choices(numbers, k=length))
    return random_numbers

def generate_random_data():
    """Generate random data for the CSV."""

    logging.info(f"Generated random data ... ")

    return {
        "user_id": generate_random_numeric_string(10),
        "action": "click",
        "timestamp": str(fake.date_time_this_year()),
        "details": {
            "page": fake.chrome(),
            "uri": fake.uri_page()
        }
    }


def send_data_to_firehose(stream_name):
    # Fetch AWS credentials from environment variables
    aws_access_key_id = os.getenv('access_key_id')
    aws_secret_access_key = os.getenv('secret_access_key')
    aws_region = 'us-east-2'

    # Initialize a Firehose client with credentials from .env file
    firehose_client = boto3.client(
        'firehose',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=aws_region
    )

    # Prepare the data to send
    json_data = json.dumps(generate_random_data()).encode('utf-8')

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
    firehose_stream_name = "PUT-S3-HwC6m"


    while True:
        try:
            send_data_to_firehose(firehose_stream_name)
        except Exception as e:
            logging.error(f"An error occurred: {e}")

        time_to_sleep = random.randint(1, 10)
        logging.info(f"Sleeping for {time_to_sleep} seconds before the next iteration")
        time.sleep(time_to_sleep)
