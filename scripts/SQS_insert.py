import boto3
import json
import os
from dotenv import load_dotenv

# Set up your AWS Elasticsearch (OpenSearch) domain endpoint and region
load_dotenv()

QUEUE_URL = os.getenv("AWS_SQS") 

def send_message_to_sqs():
    # Create an SQS client
    sqs = boto3.client('sqs', region_name='us-east-1')
    
    # Prepare your message payload as a JSON string
    message_body = json.dumps({
        "city": "new york",
        "cuisine": "chinese",
        "partySize":"6",
        "time": "13:00",
        "email":"bengochea003@gmail.com"
    })
    
    # Send the message to the SQS queue
    response = sqs.send_message(
        QueueUrl=QUEUE_URL,
        MessageBody=message_body
    )
    
    # Print out the response from SQS
    print("Message sent! Message ID:", response.get("MessageId"))

if __name__ == "__main__":
    send_message_to_sqs()
