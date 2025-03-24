import json
import boto3

def lambda_handler(event, context):
    # --- Extract the message from the API request body ---
    try:
        body = json.loads(event.get('body', '{}'))
        messages = body.get('messages', [])
        # Expecting messages to be a list containing an object with key 'unstructured'
        if messages and isinstance(messages, list) and 'unstructured' in messages[0]:
            input_text = messages[0]['unstructured'].get('text', '')
        else:
            input_text = ''
    except Exception as e:
        return {
            'statusCode': 400,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({'error': 'Invalid request format'})
        }
    
    if not input_text:
        return {
            'statusCode': 400,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({'error': 'No message provided'})
        }
    

    # --- Call Lex using the extracted message ---
    lex_client = boto3.client('lex-runtime', region_name='us-east-1')
    try:
        lex_response = lex_client.post_text(
            botName='BookTrip',             
            botAlias='DiningSuggestion',    
            userId='user1',               
            inputText=input_text
        )
    except Exception as e:
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({'error': str(e)})
        }
    

    # --- Extract Lex's response message ---
    lex_message = lex_response.get('message', 'No response from Lex')
    
    
    # --- Build the response in the expected frontend format ---
    # Frontend expects a key 'messages' containing a list of message objects.
    response_body = {
        'messages': [
            {
                'type': 'unstructured',
                'unstructured': {
                    'text': lex_message
                }
            }
        ]
    }
    
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        },
        'body': json.dumps(response_body)
    }



