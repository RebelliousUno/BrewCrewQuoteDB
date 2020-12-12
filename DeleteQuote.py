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


def handle_delete(search, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('Serge_quotes')
    response = table.delete_item(
        Key={
            'id': search
        }
    )
    return response


def get_api_key():
    client = boto3.client('ssm')
    param = client.get_parameter(Name='sergeApiToken')
    return param['Parameter']['Value']


def lambda_handler(event, context):
    api_key = get_api_key()

    key = event['queryStringParameters']['key']
    if not key or key != api_key:
        return {
            'statusCode': 403,
            'body': "Missing API Key"

        }

    search = event['queryStringParameters']['search'].strip()
    searchType = parse_search(search)
    if searchType != Search.ID:
        return {
            'statusCode': 400,
            'body': 'Delete Quote missing ID'
        }

    result = handle_delete(search)
    if result:
        return {
            'statusCode': 200,
            'body': 'Quote deleted'
        }
    return {
        'statusCode': 500,
        'body': 'Quote not found or not deleted'
    }

