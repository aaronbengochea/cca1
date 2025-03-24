import requests
import os
from requests_aws4auth import AWS4Auth
import boto3
from boto3.dynamodb.conditions import Attr
from dotenv import load_dotenv

# Load env variables
load_dotenv()

# Set up your AWS Elasticsearch (OpenSearch) domain endpoint and region
ES_ENDPOINT = os.getenv("AWS_ES")  # Replace with your ES domain URL
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

def get_new_york_restaurants():
    dynamodb = boto3.resource('dynamodb', region_name=region)
    table = dynamodb.Table('yelp-restaurants')
    response = table.scan(FilterExpression=Attr('city').eq("New York"))
    return response.get('Items', [])

def index_restaurant_to_es(restaurant):
    business_id = restaurant.get("businessId")
    cuisine = restaurant.get("cuisine")
    doc = {"RestaurantID": business_id, "Cuisine": cuisine}
    url = f"{ES_ENDPOINT}/{INDEX}/_doc/{business_id}"
    response = requests.put(url, auth=awsauth, json=doc)
    if response.status_code in (200, 201):
        print(f"Indexed {business_id} successfully.")
    else:
        print(f"Error indexing {business_id}: {response.status_code} {response.text}")

def main():
    print(ES_ENDPOINT)
    print("script started \n")
    restaurants = get_new_york_restaurants()
    print(f"Found {len(restaurants)} restaurants in New York.")
    for restaurant in restaurants:
        index_restaurant_to_es(restaurant)

if __name__ == "__main__":
    main()
