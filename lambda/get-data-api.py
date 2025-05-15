import json
import requests
import boto3
from datetime import datetime
import os

s3 = boto3.client('s3')

WEATHER_API_KEY = os.environ.get('WEATHER_API_KEY')
BUCKET_NAME = os.environ.get('BUCKET_NAME', 'weather-datalake-projects-scripts')
PREFIX = 'raw/'

CITIES = ['Paris', 'London', 'Dakar']

def lambda_handler(event, context):
    for city in CITIES:
        url = f"http://api.weatherapi.com/v1/current.json?key={WEATHER_API_KEY}&q={city}&aqi=no"
        response = requests.get(url)
        data = response.json()
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        key = f"{PREFIX}{city.lower()}_{timestamp}.json"

        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=key,
            Body=json.dumps(data),
            ContentType='application/json'
        )

    return {
        'statusCode': 200,
        'body': f"Weather data for {', '.join(CITIES)} saved to S3."
    }
