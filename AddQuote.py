import json
import re
import urllib.parse
from decimal import Decimal
import boto3
from boto3.dynamodb.conditions import Key

def getApiKey():
    client = boto3.client('ssm')
    param = client.get_parameter(Name='sergeApiToken')
    return param['Parameter']['Value']

def addQuote(quote, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table('Serge_quotes')
        response = table.query(
            KeyConditionExpression=Key('id').eq(-1)
        )
        print(response['Items'])
        quote['id'] = response['Items'][0]['val']
    
        
        table.put_item(
            Item = {
                'id': quote['id'],
                'quote': quote['quote'],
                'quote_lower': quote['quote'].lower(),
                'author': quote['author'],
                'author_lower': quote['author'].lower(),
                'date': quote['date']
            }
        )
        update_item = table.update_item(
        Key={
            'id': -1
        },
        UpdateExpression="set val = val + :update",
        ExpressionAttributeValues={
            ':update': Decimal(1)
        },
        ReturnValues="UPDATED_NEW"
    )
    return quote['id']

def parseQuote(quote): 
    x = re.match("(?P<quote>"".*"")-(?P<author>.*),(?P<date>.*)", quote)
        # potential Regex (?P<quote>[\"\*].*[\"\*])\s*-\s*(?P<author>.*)\s*,\s*(?P<date>.*)
    if x:
        return {
            'id': -1,
            'quote': x.group('quote').strip(),
            'author': x.group('author').strip(),
            'date': x.group('date').strip()
            
        }
    else:
        return None
    

def lambda_handler(event, context):
    # TODO implement
    api_key = getApiKey()

    key = event['queryStringParameters']['key']
    if not key or key != api_key:
        return {
            'statusCode': 403,
            'body': "Missing API Key"
            
        }
        
    quote = event['queryStringParameters']['quote']
    if not quote:
        return {
            'statusCode': 400,
            'body': "quote Missing"
        }
    quoteDict = parseQuote(quote)
    if not quoteDict:
        return {
            'statusCode': 500,
            'body': "Quote did not parse"
        }
        
    responses = addQuote(quoteDict)
    
    return {
        'statusCode': 200,
        'body': "Quote added with id " + str(responses)
    }
