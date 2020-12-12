import json
from enum import Enum
import re
import boto3
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.conditions import Attr
import random

class Search(Enum):
    ID = 1
    TEXT = 2
    DATE = 3
    NOMATCH = 4
    

def parse_search(search):
    idMatcher = re.compile(r"\d+")
    dateMatcher = re.compile(r"\d{4}-\d{2}-\d{2}")
    textMatchers = re.compile(r"[a-zA-Z0-9]*")
    
    if dateMatcher.search(search):
        return Search.DATE
    if idMatcher.search(search):
        return Search.ID
    if textMatchers.search(search):
        return Search.TEXT
    return Search.NOMATCH
    
def get_api_key():
    client = boto3.client('ssm')
    param = client.get_parameter(Name='sergeApiToken')
    return param['Parameter']['Value']

    
def handle_search(search, searchType, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('Serge_quotes')
    if searchType == Search.ID:
        response = table.query(
            KeyConditionExpression=Key('id').eq(int(search))
        )
    elif searchType == Search.DATE:
        response = table.query(
            IndexName = "date-id-index",
            KeyConditionExpression=Key("date").eq(search)
            )
    elif searchType == Search.TEXT:
        print(search.lower())
        quote = table.scan(
            IndexName = "quote_lower-id-index",
            FilterExpression=Attr("quote_lower").contains(search.lower())
            )
        print(quote)
        author = table.scan(
            IndexName = "author_lower-id-index",
            FilterExpression=Attr("author_lower").contains(search.lower())
            )
        if len(quote["Items"]) > 0:
            print("Found Quotes")
            response = quote
            response["Items"].extend(author["Items"])
        else:
            response = author
          
    print(response)      
    if len(response["Items"])>0:   
        resp = random.choice(response["Items"])
        return "{} - {} on {}".format(resp["quote"],resp["author"], resp["date"] )
    else:
        return "No quote found"
        
        
        

def lambda_handler(event, context):
    # TODO implement
    api_key = get_api_key()

    key = event['queryStringParameters']['key']
    if not key or key != api_key:
        return {
            'statusCode': 403,
            'body': "Missing API Key"
            
        }
    
    search = event['queryStringParameters']['search'].strip()
    searchType = parse_search(search)
    result = handle_search(search, searchType)
    print(result)
    return {
        'statusCode': 200,
        'body': result
    }
