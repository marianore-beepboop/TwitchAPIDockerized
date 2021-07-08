import os
from dotenv import load_dotenv
import requests

def get_client_credentials(type):
    load_dotenv("./")
    client_id = os.getenv('CLIENT_ID')
    client_secret = os.getenv('CLIENT_SECRET')

    if type == "id": return client_id
    elif type == "secret": return client_secret

def get_access_token():
    client_id = get_client_credentials("id")
    client_secret = get_client_credentials("secret")
    
    response_token = requests.post(f"https://id.twitch.tv/oauth2/token?client_id={client_id}&client_secret={client_secret}&grant_type=client_credentials")
    new_access_token = response_token.json()["access_token"]
    return new_access_token

headers = {
        'Authorization': f'Bearer {get_access_token()}',
        'Client-Id': get_client_credentials("id")
    }