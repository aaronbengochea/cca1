import json
import boto3
import os
from utils import elicit_slot, close, delegate, validate_dinning_parameters

QUEUE_URL = os.getenv("AWS_SQS")

# --- SQS Send Message Handler ----
def send_message_to_sqs(city, cuisine, partySize, time,  email):
    sqs = boto3.client('sqs', region_name='us-east-1')
    message_body = json.dumps({
        "city": city,
        "cuisine": cuisine,
        "partySize": partySize,
        "time": time,
        "email": email
    })
    response = sqs.send_message(
        QueueUrl=QUEUE_URL,
        MessageBody=message_body
    )
    print("SQS response:", response)
    return response


# --- Greeting Intent Handler ----
def greeting_intent(intent_request):
    session_attributes = intent_request.get('sessionAttributes' , {})

    message = "Hello! I am the Dining Concierge Bot, how can I help?"

    return close(session_attributes, 'Fulfilled', message)


# --- Thankyou Intent Handler ----
def thankyou_intent(intent_request):
    session_attributes = intent_request.get('sessionAttributes' , {})

    message = "You are welcome! Please be sure to check your email shortly for your personalized dining suggestions!"

    return close(session_attributes, 'Fulfilled', message)


# --- Dining Suggestion Intent Handler ---
def suggest_dining(intent_request):
    """Handle the booking of a hotel."""
    slots = intent_request['currentIntent']['slots']
    session_attributes = intent_request['sessionAttributes'] if intent_request['sessionAttributes'] is not None else {}

    # Extract slot values
    city = slots['City']
    cuisine = slots['Cuisine']
    partySize = slots['PartySize']
    time = slots['Time']
    email = slots['Email']

    # Validate the slots using the helper function from utils.py
    validation_result = validate_dinning_parameters(city, cuisine, partySize, time, email)
    if not validation_result['isValid']:
        return elicit_slot(session_attributes, 
                           intent_request['currentIntent']['name'], 
                           slots, 
                           validation_result['violatedSlot'], 
                           validation_result['message']['content'])

    # If the function is called in DialogCodeHook, delegate control back to Lex
    if intent_request['invocationSource'] == 'DialogCodeHook':
        return delegate(session_attributes, slots)

    print("SQS variables collected", city, cuisine, time, partySize, email)
    send_message_to_sqs(city, cuisine, partySize, time, email)

    # Fulfillment message after validation and confirmation
    message = (f"Thank you! The request is currently being processed. Please check your email shortly for personalized dining suggestions!")
    
    return close(session_attributes, 'Fulfilled', message)


# --- Intent Dispatcher ---
def dispatch(intent_request):
    """Route to the appropriate intent handler."""
    intent_name = intent_request['currentIntent']['name']

    if intent_name == 'GreetingIntent':
        print("GreetingIntent triggered")
        return greeting_intent(intent_request)

    if intent_name == 'ThankYouIntent':
        return thankyou_intent(intent_request)

    if intent_name == 'DiningSuggestionIntent':
        print("DiningSuggestionIntent triggered")
        return suggest_dining(intent_request)

    raise Exception(f"Intent with name {intent_name} not supported")


# --- Main Lambda Handler ---
def lambda_handler(event, context):
    return dispatch(event)