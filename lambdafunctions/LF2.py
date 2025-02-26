import json
import boto3
import random
import requests
from requests_aws4auth import AWS4Auth

# Set up your Elasticsearch domain and index.
ES_ENDPOINT = "https://search-hw1-pwgm55hunr22zboasjqmkbj4ai.aos.us-east-1.on.aws"  # Your ES endpoint
INDEX = "restaurants"
region = 'us-east-1'
service = 'es'

# SQS Queue URL for receiving suggestion requests.
QUEUE_ENDPOINT = "https://sqs.us-east-1.amazonaws.com/376129866895/restaurantsQueue"


def lambda_handler(event, context):
    print("LF2 Lambda invoked.")

    # Initialize AWS clients.
    sqs = boto3.client('sqs', region_name=region)
    dynamodb = boto3.resource('dynamodb', region_name=region)
    ses = boto3.client('ses', region_name=region)
    
    # Retrieve one message from SQS.
    print("Polling SQS for messages...")
    sqs_response = sqs.receive_message(
        QueueUrl=QUEUE_ENDPOINT,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=0
    )
    print("SQS response:", json.dumps(sqs_response, indent=2))
    
    if 'Messages' not in sqs_response:
        print("No messages in queue.")
        return {"status": "No messages in queue"}
    
    message = sqs_response['Messages'][0]
    receipt_handle = message['ReceiptHandle']
    print("Received message. Receipt Handle:", receipt_handle)
    
    # Parse SQS message body.
    try:
        body = json.loads(message['Body'])
        city = body.get('city')
        cuisine = body.get('cuisine')
        partySize = body.get('partySize')
        time_str = body.get('time')
        email = body.get('email')
        print("Extracted fields:", {"city": city, "cuisine": cuisine, "partySize": partySize, "time": time_str, "email": email})
    except Exception as e:
        print("Error parsing SQS message:", str(e))
        sqs.delete_message(QueueUrl=QUEUE_ENDPOINT, ReceiptHandle=receipt_handle)
        return {"status": "Error parsing message", "error": str(e)}
    
    if not (city and cuisine and email):
        print("Missing required fields. Deleting message.")
        sqs.delete_message(QueueUrl=QUEUE_ENDPOINT, ReceiptHandle=receipt_handle)
        return {"status": "Incomplete message data", "error": "Missing city, cuisine, or email."}
    
    # ---- Query Elasticsearch for restaurants matching city and cuisine ----
    # Build an ES query to match both "Cuisine" and "City".
    query = {
        "size": 10,
        "query": {
            "match": {
                "Cuisine": cuisine
            }
        }
    }
    
    # Build the URL.
    url = f"{ES_ENDPOINT}/{INDEX}/_search"

    # Define additional headers.
    headers = {
        "Content-Type": "application/json"
    }

    # Send the GET request to Elasticsearch.
    response = requests.get(url, auth=("abengo007", "Aaron003$"), headers=headers, json=query)
    
    results = response.json()
    hits = results.get('hits', {}).get('hits', [])
    print(hits)

    if not hits:
        print("No matching restaurants found in ES. Deleting SQS message.")
        sqs.delete_message(QueueUrl=QUEUE_ENDPOINT, ReceiptHandle=receipt_handle)
        return {"status": "No restaurants found in ES"}
    
    # Extract restaurant IDs from ES results.
    restaurant_ids = []
    for hit in hits:
        source = hit.get('_source', {})
        rid = source.get('RestaurantID')  # Assumes ES stores the businessId as "RestaurantID"
        if rid:
            restaurant_ids.append(rid)
    
    print("Restaurant IDs from ES:", restaurant_ids)
    
    # Select 3 random restaurant IDs.
    if len(restaurant_ids) < 3:
        selected_ids = restaurant_ids
    else:
        selected_ids = random.sample(restaurant_ids, 3)
    
    print("Selected restaurant IDs:", selected_ids)


    
    # ---- Retrieve additional restaurant details from DynamoDB ----
    table = dynamodb.Table('yelp-restaurants')
    print("connected to dynamo")
    recommendations = []
    for rid in selected_ids:
        try:
            ddb_resp = table.get_item(Key={'businessId': rid})
            if 'Item' in ddb_resp:
                recommendations.append(ddb_resp['Item'])
        except Exception as e:
            print(f"Error retrieving DynamoDB item for {rid}:", str(e))
            continue
    
    print("Restaurant recommendations from DynamoDB:", recommendations)
    
    # ---- Build an email template ----
    if not recommendations:
        email_body = (f"Hello,\n\nWe couldn't find any restaurant recommendations for {cuisine} cuisine in {city}.\n"
                      "Please try again later.\n\nRegards,\nDining Concierge Bot")
    else:
        email_body = f"Hello,\n\nHere are my three {cuisine} restaurant recommendations for {partySize} people, today, in {city}:\n\n"
        for rec in recommendations:
            name = rec.get('name', 'Unknown')
            address = rec.get('address', 'Address not available')
            rating = rec.get('rating', 'N/A')
            email_body += f"Name: {name}\nAddress: {address}\nRating: {rating}\n\n"
        email_body += "Enjoy your meal!\n\nRegards,\nDining Concierge Bot"
    
    print("Built email body:")
    print(email_body)
    print(email)
    
    # ---- Send the email via SES ----
    # Update Source with a verified email address in SES.
    try:
        ses_response = ses.send_email(
            Source="aabengo00345@gmail.com",
            Destination={'ToAddresses': [email]},
            Message={
                'Subject': {
                    'Data': f"Your {cuisine} Restaurant Recommendations",
                    'Charset': 'UTF-8'
                },
                'Body': {
                    'Text': {
                        'Data': email_body,
                        'Charset': 'UTF-8'
                    }
                }
            }
        )
        print("SES email sent successfully. Response:", ses_response)
    except Exception as e:
        print("Error sending email via SES:", str(e))
        return {"status": "Error sending email", "error": str(e)}
    
    # ---- Delete the SQS message after processing ----
    print("Deleting the processed SQS message...")
    sqs.delete_message(QueueUrl=QUEUE_ENDPOINT, ReceiptHandle=receipt_handle)
    print("SQS message deleted.")
    
    return {
        "status": "Success",
        "recommendations": recommendations,
        "ses_response": ses_response
    }