# --- Helper Functions for Lex Responses ---
import datetime as datetime
import re

# --- Lex Response Helper Functions ---
def elicit_slot(session_attributes, intent_name, slots, slot_to_elicit, message):
    """Elicit the next slot value."""
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ElicitSlot',
            'intentName': intent_name,
            'slots': slots,
            'slotToElicit': slot_to_elicit,
            'message': {'contentType': 'PlainText', 'content': message}
        }
    }

def confirm_intent(session_attributes, intent_name, slots, message):
    """Confirm the user's intent."""
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ConfirmIntent',
            'intentName': intent_name,
            'slots': slots,
            'message': {'contentType': 'PlainText', 'content': message}
        }
    }

def close(session_attributes, fulfillment_state, message):
    """Close the conversation with a completion message."""
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': fulfillment_state,
            'message': {'contentType': 'PlainText', 'content': message}
        }
    }

def delegate(session_attributes, slots):
    """Delegate control to Lex for the next step in the conversation."""
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Delegate',
            'slots': slots
        }
    }
        
def build_validation_result(is_valid, violated_slot, message_content):
    """Build the result structure for validation."""
    if not is_valid:
        return {
            'isValid': False,
            'violatedSlot': violated_slot,
            'message': {'contentType': 'PlainText', 'content': message_content}
        }
    return {'isValid': True}

# --- Slot Validation Functions ---
def is_valid_city(city):
    """Check if the city is valid for this bot."""
    valid_cities = ['new york','los angeles']
    return city.lower() in valid_cities

def is_valid_cuisine(cuisine):
    """Check if the cuisine is valid for this bot."""
    # Adjust this list to your needs:
    valid_cuisines = ['american', 'mexican', 'chinese']
    return cuisine.lower() in valid_cuisines

def is_party_valid(party_size):
    """
    Check if the partySize is valid for this bot.
    Return a 2-tuple: (bool: isValid, str: message).
    """
    if party_size < 1:
        return (False, "Party must have at least 1 person.")
    if party_size > 10:
        return (False, "Party is too large. Please keep it 10 or fewer.")
    return (True, "Success")

def is_time_valid(lex_time_str):
    """
    Checks if the given lex_time_str (Amazon Lex AMAZON.TIME format) 
    is between 9:00 (09:00) and 22:00 (10 PM).

    Lex often returns date/time in an ISO-8601-like format, e.g. '2023-01-15T09:00'
    or just '09:00' (if no date is available).

    :param lex_time_str: The string from Lex, e.g. "2023-01-15T09:00" or "09:00".
    :return: dict with keys "isValid" (bool) and "message" (str).
    """
    if lex_time_str:
        return {
            "isValid": False,
            "message": {lex_time_str}
        }

        
    try:
        if 'T' in lex_time_str:
            time_part = lex_time_str.split('T')[-1]
        else:
            time_part = lex_time_str  

        time_obj = datetime.strptime(time_part, "%H:%M")
        hour = time_obj.hour

        if hour < 9 or hour >= 22:
            return {
                "isValid": False,
                "message": "Time must be between 9:00 and 22:00."
            }
        return {
            "isValid": True,
            "message": "Success"
        }

    except ValueError:
        return {
            "isValid": False,
            "message": (
                "Invalid time format. Please specify a time between "
                "9:00 (09:00) and 22:00 (10 PM) in HH:MM (24-hour) format."
            )
        }



# --- Main Validation Function ---
def validate_dinning_parameters(city, cuisine, partySize, time, email):
    """Perform validation for all slots with advanced checks."""
    if city and not is_valid_city(city):
        return build_validation_result(False, 'City', f"We do not support {city} yet. Please choose from: New York, or Los Angeles.")
    

    if cuisine and not is_valid_cuisine(cuisine):
        return build_validation_result(False, 'Cuisine', f"We do not support {cuisine} yet. Please choose from: American, Chinese, or Mexican.")

    if partySize:
        party_valid, party_message = is_party_valid(int(partySize))
        if not party_valid:
            return build_validation_result(False, 'PartySize', party_message)

    if time:
        time_valid, time_message = is_time_valid(time)
        if not time_valid:
            return build_validation_result(False, 'Time', time_message)

    return {'isValid': True, 'violatedSlot': None, 'message': None}