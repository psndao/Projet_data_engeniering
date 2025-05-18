# importer les bibliothèques nécessaires
import json
import requests
import boto3
from datetime import datetime
import os

# Récupérer la clé API météo depuis les variables d’environnement Lambda
WEATHER_API_KEY = os.environ.get('xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')  
BUCKET_NAME = os.environ.get('xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')
PREFIX = 'raw/'
CITIES = ['Paris', 'London', 'Dakar']

# Initialiser client S3
s3 = boto3.client('s3')

def lambda_handler(event, context):
    for city in CITIES:
        url = f"http://api.weatherapi.com/v1/current.json?key={WEATHER_API_KEY}&q={city}&aqi=no"
        response = requests.get(url)

        if response.status_code != 200:
            print(f"Erreur API pour {city} : {response.status_code} - {response.text}")
            continue

        data = response.json()
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        key = f"{PREFIX}{city.lower()}_{timestamp}.json"

        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=key,
            Body=json.dumps(data),
            ContentType='application/json'
        )

        print(f" {city} sauvegardée : s3://{BUCKET_NAME}/{key}")

    return {
        'statusCode': 200,
        'body': f"Données météo sauvegardées pour : {', '.join(CITIES)}"
    }
