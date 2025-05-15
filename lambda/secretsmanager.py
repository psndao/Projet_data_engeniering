import boto3
import json
import os

def get_secret(secret_name):
    region_name = os.environ.get('AWS_REGION', 'us-east-1')
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)

    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response['SecretString'])['weather_api_key']
