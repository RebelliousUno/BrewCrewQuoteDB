import re
from decimal import Decimal
import boto3
from boto3.dynamodb.conditions import Key


def get_api_key():
    client = boto3.client('ssm')
    param = client.get_parameter(Name='sergeApiToken')
    return param['Parameter']['Value']


def add_quote(quote, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table('Serge_quotes')
        response = table.query(
            KeyConditionExpression=Key('id').eq(-1)
        )
        print(response['Items'])
        quote['id'] = response['Items'][0]['val']

        table.put_item(
            Item={
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
            UpdateExpression='set val = val + :update',
            ExpressionAttributeValues={
                ':update': Decimal(1)
            },
            ReturnValues='UPDATED_NEW'
        )
    return quote['id']


def parse_quote(quote):
    c = re.compile('(?P<quote>["\*].*["\*])\s*-\s*(?P<author>.*)\s*,\s*(?P<date>\d{4}-\d{2}-\d{2})')
    x = c.match(quote)

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
    api_key = get_api_key()

    key = event['queryStringParameters']['key']
    if not key or key != api_key:
        return {
            'statusCode': 403,
            'body': 'Missing API Key'

        }

    quote = event['queryStringParameters']['quote']
    if not quote:
        return {
            'statusCode': 400,
            'body': 'Quote Missing'
        }
    quoteDict = parse_quote(quote)
    if not quoteDict:
        return {
            'statusCode': 500,
            'body': 'Quote did not parse'
        }

    responses = add_quote(quoteDict)
    if responses >= 0:
        return {
            'statusCode': 200,
            'body': 'Quote added with id ' + str(responses)
        }
    return {
        'statusCode': 500,
        'body': 'Could not add quote'
    }

