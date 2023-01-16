import boto3
from botocore.exceptions import ClientError
from pprint import pprint
import json
from decimal import Decimal

ACCESS_KEY = 'AKIAZSXXXNVF3DLQXI6S'
SECRET_KEY = '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'

session = boto3.Session(aws_access_key_id=ACCESS_KEY,
                        aws_secret_access_key=SECRET_KEY)
dynamodb = session.resource('dynamodb', region_name='eu-west-2')


def createTable():
    print('Create Table')

    table = dynamodb.create_table(
        TableName='books',
        KeySchema=[
            {
                'AttributeName': 'Author',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'Title',
                'KeyType': 'RANGE'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'Author',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'Title',
                'AttributeType': 'S'
            },
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )

    print(table.table_status)
    table.wait_until_exists()
    print(table.table_status)

def addData():
    with open('books_data.json') as json_file:
        books_list = json.load(json_file, parse_float=Decimal)

    table = dynamodb.Table('books')

    for book in books_list:
        print(book['Title'])
        table.put_item(Item=book)

def insertItem():
    table = dynamodb.Table('books')

    response = table.put_item(
        Item = {
            'Author': 'Murtaza',
            'Title': 'The Big New Book',
            'Category': 'Thriller',
            'Formats': {
                'Hardcover': 'J4RSSUKW',
                'Paperback': 'DTUY45Z'
            }
        }
    )

    print(response)

def readItem():
    table = dynamodb.Table('books')

    author = 'Murtaza'
    title = 'The Big New Book'

    try:
        response = table.get_item(Key={'Author':author, 'Title':title})
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print(response['Item'])

def updateItem():
    table = dynamodb.Table('books')
    response = table.update_item()