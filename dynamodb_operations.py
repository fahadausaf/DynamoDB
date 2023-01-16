import boto3

ACCESS_KEY = 'AKIAZSXXXNVF3DLQXI6S'
SECRET_KEY = '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'
#dynamodb = boto3.resource('dynamodb')
session = boto3.Session(aws_access_key_id=ACCESS_KEY,
                        aws_secret_access_key=SECRET_KEY)
dynamodb = session.resource('dynamodb', region_name='eu-west-2')


def createTable():
    # Get the service resource.
    dynamodb = boto3.resource('dynamodb')

    # Create the DynamoDB table.
    table = dynamodb.create_table(
        TableName='users',
        KeySchema=[
            {
                'AttributeName': 'username',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'last_name',
                'KeyType': 'RANGE'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'username',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'last_name',
                'AttributeType': 'S'
            },
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )

    # Wait until the table exists.
    table.wait_until_exists()

    # Print out some data about the table.
    print(table.item_count)


def insertTable():
    table = dynamodb.Table('users')
    # print(table.creation_date_time)

    table.put_item(
        Item={
            'username': 'fahadausaf',
            'first_name': 'fahad',
            'last_name': 'ausaf',
            'age': 25,
            'account_type': 'standard_user',
        }
    )


def readTable():
    table = dynamodb.Table('users')
    response = table.get_item(
        Key={
            'username': 'fahadausaf',
            'last_name': 'ausaf'
        }
    )
    item = response['Item']
    print(item)