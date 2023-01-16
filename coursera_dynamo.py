import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
from pprint import pprint
import json
from decimal import Decimal

ACCESS_KEY = ''
SECRET_KEY = ''

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
    except Exception as e: 
        print(e)
    else:
        print(response['Item'])

def updateItem():
    table = dynamodb.Table('books')
    author = 'Murtaza'
    title = 'The Big New Book'
    category = 'Suspense'

    response = table.update_item(
        Key = {
            'Author': author,
            'Title': title
        },
        UpdateExpression = 'SET Category=:c',
        ExpressionAttributeValues = {
            ':c':category
        },
        ReturnValues = 'UPDATED_NEW'
    )

    print(response)

def deleteItem1():
    table = dynamodb.Table('books')
    author = 'Murtaza'
    title = 'The Big New Book'
    category = 'Suspense'

    response = table.delete_item(
        Key = {
            'Author': author,
            'Title': title
        },
        ConditionExpression='Category=:val',
        ExpressionAttributeValues={
            ':val': category
        })
    print(response)

def deleteItem2():
    table = dynamodb.Table('books')
    author = 'Murtaza'
    title = 'The Big New Book'
    category = 'Suspense'

    response = table.delete_item(
        Key = {
            'Author': author,
            'Title': title
        })
    print(response)

def query():
    table = dynamodb.Table('books')
    response = table.query(
        ProjectionExpression = 'Title,Category,#ft',
        ExpressionAttributeNames = {'#ft':'format'},
        KeyConditionExpression = 
        Key('Author').eq('John Grisham')
    )

    print(response)

def scan():
    table = dynamodb.Table('books')

    scan_kwargs = {
        'FilterExpression': Key('Category').eq('Suspense'),
        'ProjectionExpression': 'Title, Category, #ft',
        'ExpressionAttributeNames': {'#ft':'format'}
    }

    done = False
    start_key = None

    while not done:
        if start_key:
            scan_kwargs['ExclusiveStartKey'] = start_key
        response = table.scan(**scan_kwargs)
        print(response)
        start_key = response.get('LastEvaluatedKey', None)
        done = start_key is None

    print(response)

