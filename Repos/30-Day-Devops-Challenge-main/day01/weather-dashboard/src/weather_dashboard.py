import os
import json
import boto3
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Constants
API_KEY = os.getenv("OPENWEATHER_API_KEY")
BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
AWS_REGION = "us-east-1"  # Adjust based on your region
WEATHER_API_URL = "https://api.openweathermap.org/data/2.5/weather"

# AWS S3 Client
s3_client = boto3.client("s3", region_name=AWS_REGION)

def fetch_weather(city):
    """Fetch weather data for a given city using OpenWeather API."""
    params = {"q": city, "appid": API_KEY, "units": "imperial"}
    response = requests.get(WEATHER_API_URL, params=params)
    
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Error fetching weather data: {response.status_code}, {response.text}")

def upload_to_s3(data, filename):
    """Upload weather data to S3 bucket."""
    try:
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=filename,
            Body=json.dumps(data),
            ContentType="application/json"
        )
        print(f"Uploaded {filename} to bucket {BUCKET_NAME}")
    except Exception as e:
        print(f"Error uploading to S3: {e}")

def main():
    cities = ["New York", "Los Angeles", "Chicago","pakistan"]  # Add more cities as needed
    for city in cities:
        print(f"Fetching weather data for {city}...")
        weather_data = fetch_weather(city)
        
        filename = f"weather_{city.replace(' ', '_').lower()}.json"
        upload_to_s3(weather_data, filename)

if __name__ == "__main__":
    main()

