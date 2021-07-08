import requests
from config import headers

def get_games_data():
    response = requests.get("https://api.twitch.tv/helix/games/top?first=100", headers=headers)
    games_data = response.json()
    return games_data

def get_streams_data():
    response = requests.get("https://api.twitch.tv/helix/streams?first=100", headers=headers)
    streams_data = response.json()
    return streams_data