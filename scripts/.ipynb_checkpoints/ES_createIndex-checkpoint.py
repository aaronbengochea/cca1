import json
import requests
from requests_aws4auth import AWS4Auth
import boto3
import os
from dotenv import load_dotenv

# Load env variables
load_dotenv()

# Set up your AWS Elasticsearch (OpenSearch) domain endpoint and region
ES_ENDPOINT = os.getenv("AWS_ES")  
INDEX = "restaurants"  
region = 'us-east-1'
service = 'es'

# Get AWS credentials using boto3
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key,
                   credentials.secret_key,
                   region,
                   service,
                   session_token=credentials.token)

def create_index():
    """
    Creates an Elasticsearch index called "restaurants" with the mapping for RestaurantID and Cuisine.
    Note: We remove the custom type name and define properties directly.
    """
    print(ES_ENDPOINT)

    url = f"{ES_ENDPOINT}/{INDEX}"
    mapping = {
        "mappings": {
            "properties": {
                "RestaurantID": {"type": "keyword"},
                "Cuisine": {"type": "keyword"}
            }
        }
    }
    response = requests.put(url, auth=awsauth, json=mapping)
    if response.status_code in (200, 201):
        print("Index created successfully.")
    else:
        print(f"Error creating index: {response.status_code} {response.text}")

def index_restaurant_to_es(restaurant):
    """
    Indexes a single restaurant document in Elasticsearch.
    The document only contains the RestaurantID and Cuisine fields.
    Uses the default _doc type.
    """
    business_id = restaurant.get("businessId")
    cuisine = restaurant.get("cuisine")
    
    doc = {
        "RestaurantID": business_id,
        "Cuisine": cuisine
    }
    
    # Using _doc as the document type (the default in ES 7+)
    url = f"{ES_ENDPOINT}/{INDEX}/_doc/{business_id}"
    response = requests.put(url, auth=awsauth, json=doc)
    
    if response.status_code in (200, 201):
        print(f"Indexed {business_id} successfully.")
    else:
        print(f"Error indexing {business_id}: {response.status_code} {response.text}")



if __name__ == "__main__":
    create_index()



