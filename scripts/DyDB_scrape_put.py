import time
import requests
import boto3
import os
from datetime import datetime
from decimal import Decimal
from dotenv import load_dotenv

# Load env variables
load_dotenv()

# Replace with your actual Yelp API key
YELP_API_KEY = os.getenv("YELP_KEY")

# Initialize the DynamoDB resource (ensure AWS credentials and region are set)
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('yelp-restaurants')

# Define cuisines and cities
CUISINES = ["chinese", "mexican", "american"]

# List of cities with location string, and explicit city and state values
CITIES = [
    {"location": "New York, NY", "city": "New York", "state": "NY"},
    {"location": "Los Angeles, CA", "city": "Los Angeles", "state": "CA"}
]

# Yelp API endpoint & headers
YELP_SEARCH_URL = "https://api.yelp.com/v3/businesses/search"
HEADERS = {
    "Authorization": f"Bearer {YELP_API_KEY}"
}

def convert_floats_to_decimal(obj):
    """
    Recursively walks a nested dict/list and converts all floats to Decimal.
    """
    if isinstance(obj, float):
        return Decimal(str(obj))
    elif isinstance(obj, dict):
        return {k: convert_floats_to_decimal(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_floats_to_decimal(e) for e in obj]
    else:
        return obj

def fetch_restaurants_by_cuisine(cuisine, location, limit=50):
    """
    Fetch restaurants from Yelp for a given cuisine and location.
    We'll try to gather up to 'limit' results.
    """
    params = {
        "term": f"{cuisine} restaurants",
        "location": location,
        "limit": limit
    }
    response = requests.get(YELP_SEARCH_URL, headers=HEADERS, params=params)
    data = response.json()

    if 'businesses' not in data:
        print(f"Error fetching from Yelp for {cuisine} in {location}: {data}")
        return []
    
    return data["businesses"]

def store_in_dynamodb(restaurant_data, cuisine, city_info):
    """
    Store the relevant restaurant info in DynamoDB table 'yelp-restaurants'.
    Extracts:
      - Business ID, Name, Address, Coordinates, Number of Reviews, Rating, Zip Code,
        and explicitly stores the city and state as separate fields.
    """
    business_id = restaurant_data.get('id')
    name = restaurant_data.get('name')
    location_info = restaurant_data.get('location', {})
    address = ", ".join(location_info.get('display_address', []))
    coordinates = restaurant_data.get('coordinates', {})
    review_count = restaurant_data.get('review_count')
    rating = restaurant_data.get('rating')
    zip_code = location_info.get('zip_code')
    insertedAtTimestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    # Build the item including explicit "city" and "state" fields
    item = {
        'businessId': business_id,
        'name': name,
        'address': address,
        'coordinates': {
            'latitude': coordinates.get('latitude'),
            'longitude': coordinates.get('longitude')
        },
        'reviewCount': review_count,
        'rating': rating,
        'zipCode': zip_code,
        'insertedAtTimestamp': insertedAtTimestamp,
        'cuisine': cuisine,
        'city': city_info['city'],    # Explicit separate city field
        'state': city_info['state']   # Explicit separate state field
    }

    # Convert any float types to Decimal for DynamoDB compatibility
    item = convert_floats_to_decimal(item)

    try:
        table.put_item(Item=item)
        print(f"Stored: {business_id} - {name} ({city_info['city']}, {city_info['state']})")
    except Exception as e:
        print(f"Error storing {business_id}: {e}")

def collect_and_store_restaurants():
    """
    For each city (New York and Los Angeles) and for each cuisine,
    fetch 50 restaurants from Yelp and store them in DynamoDB.
    This will result in 150 restaurants per city (50 per cuisine).
    """
    total_saved = 0

    for city_info in CITIES:
        location = city_info['location']
        for cuisine in CUISINES:
            print(f"--- Fetching {cuisine} restaurants in {location} ---")
            restaurants = fetch_restaurants_by_cuisine(cuisine, location, limit=50)
            for restaurant in restaurants:
                store_in_dynamodb(restaurant, cuisine, city_info)
                total_saved += 1
            print(f"Saved {len(restaurants)} items for {cuisine} restaurants in {location}")

    print(f"Total saved across all cities and cuisines: {total_saved}")

if __name__ == "__main__":
    collect_and_store_restaurants()

