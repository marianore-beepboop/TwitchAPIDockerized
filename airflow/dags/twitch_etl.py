import datetime, pytz
import sqlalchemy
import pandas as pd
import requests
import json
import os
from dotenv import load_dotenv
# import sqlite3

def jprint(obj):
    # formatted string of the JSON object
    text = json.dumps(obj, sort_keys=True, indent=4)
    print(text)

def get_client_credentials(type):
    load_dotenv()
    client_id = os.getenv['client_id']
    client_secret = os.getenv['client_secret']

    if type == "id": return client_id
    elif type == "secret": return client_secret

def get_access_token():
    client_id = get_client_credentials("id")
    client_secret = get_client_credentials("secret")
    
    response_token = requests.post(f"https://id.twitch.tv/oauth2/token?client_id={client_id}&client_secret={client_secret}&grant_type=client_credentials")
    new_access_token = response_token.json()["access_token"]
    return new_access_token

def check_if_valid_data(df: pd.DataFrame) -> bool:
    # Check if dataframe is empty
    if df.empty:
        print("No stream data. Finishing execution")
        return False 

    # Check for nulls
    if df.isnull().values.any():
        raise Exception("Null values found")

    return True


def run_twitch_etl():
    headers = {
        'Authorization': f'Bearer {get_access_token()}',
        'Client-Id': get_client_credentials("id")
    }

    # * Get Top 100 Games at the moment
    response = requests.get("https://api.twitch.tv/helix/games/top?first=100", headers=headers)
    games_data = response.json()

    date_string = datetime.datetime.now(pytz.timezone('America/Argentina/Buenos_Aires'))
    game_timestamp = []

    game_id = []
    game_name = []

    for game in games_data["data"]:
        game_timestamp.append(date_string.strftime("%d/%m/%Y %H:%M:%S"))
        game_id.append(game["id"])
        game_name.append(game["name"])

    games_dict = {
        "game_timestamp": game_timestamp,
        "game_id": game_id,
        "game_name": game_name
    }

    games_df = pd.DataFrame(games_dict)
    #print(games_df.head())


    # * Get Top 100 Streams at the moment
    response = requests.get("https://api.twitch.tv/helix/streams?first=100", headers=headers)
    streams_data = response.json()

    #date_string = datetime.datetime.now(pytz.timezone('America/Argentina/Buenos_Aires'))
    stream_timestamp = []

    stream_id = []
    stream_user_id = []
    stream_user_name = []
    stream_game_id = []
    stream_game_name = []
    stream_type = []
    stream_title = []
    stream_viewer_count = []
    stream_started_at = []
    stream_language = []
    stream_tag_ids = []
    stream_is_mature = []

    for stream in streams_data["data"]:
        stream_timestamp.append(date_string.strftime("%d/%m/%Y %H:%M:%S"))
        stream_id.append(stream["id"])
        stream_user_id.append(stream["user_id"])
        stream_user_name.append(stream["user_name"])
        stream_game_id.append(stream["game_id"])
        stream_game_name.append(stream["game_name"])
        stream_type.append(stream["type"])
        stream_title.append(stream["title"])
        stream_viewer_count.append(stream["viewer_count"])
        stream_started_at.append(stream["started_at"])
        stream_language.append(stream["language"])
        stream_tag_ids.append(stream["tag_ids"])
        stream_is_mature.append(stream["is_mature"])

    streams_dict = {
        "stream_timestamp": stream_timestamp,
        "stream_id": stream_id,
        "stream_user_id": stream_user_id,
        "stream_user_name": stream_user_name,
        "stream_game_id": stream_game_id,
        "stream_game_name": stream_game_name,
        "stream_type": stream_type,
        "stream_title": stream_title,
        "stream_viewer_count": stream_viewer_count,
        "stream_started_at": stream_started_at,
        "stream_language": stream_language,
        "stream_tag_ids": stream_tag_ids,
        "stream_is_mature": stream_is_mature,
    }

    streams_df = pd.DataFrame(streams_dict)
    #print(streams_df.head())

    # * Validate DataFrames
    
    if check_if_valid_data(games_df) and check_if_valid_data(streams_df):
        print("Data valid, proceed to Load stage")

    # * Load Top Games with PostgreSQL in Docker

    DATABASE_LOCATION = "postgresql://postgres:postgres@postgres_data/db_data"
    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    games_df.to_sql("top_games", engine, index=True, if_exists="append")

    print("Data from Top Games loaded into DB")

    # * Load Top Streams with PostgreSQL in Docker

    # DATABASE_LOCATION = "postgresql://postgres:postgres@postgres_data/db_data"
    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    streams_df.to_sql("top_streams", engine, index=True, if_exists="append")

    print("Data from Top Streams loaded into DB")

    # * Load Top Games with Sqlite

    # DATABASE_LOCATION = "sqlite:///top_games.sqlite"
    # engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    # conn = sqlite3.connect('top_games.sqlite')
    # cursor = conn.cursor()

    # sql_query = """
    # CREATE TABLE IF NOT EXISTS top_games(
    #     game_timestamp VARCHAR(200),
    #     game_id VARCHAR(200),
    #     game_name VARCHAR(200),
    #     CONSTRAINT primary_key_constraint PRIMARY KEY (game_id)
    # )
    # """

    # cursor.execute(sql_query)
    # print("Opened database successfully")

    # try:
    #     games_df.to_sql("top_games", engine, index=False, if_exists='append')
    # except:
    #     print("Data already exists in the database")

    # conn.close()
    # print("Close database successfully")

    # * Load Top Streams with SQlite

    # DATABASE_LOCATION = "sqlite:///top_streams.sqlite"
    # engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    # conn = sqlite3.connect('top_streams.sqlite')
    # cursor = conn.cursor()

    # sql_query = """
    # CREATE TABLE IF NOT EXISTS top_streams(
    #     stream_timestamp VARCHAR(200),
    #     stream_id VARCHAR(200),
    #     stream_user_id VARCHAR(200),
    #     stream_user_name VARCHAR(200),
    #     stream_game_id VARCHAR(200),
    #     stream_game_name VARCHAR(200),
    #     stream_type VARCHAR(200),
    #     stream_title VARCHAR(200),
    #     stream_viewer_count VARCHAR(200),
    #     stream_started_at VARCHAR(200),
    #     stream_language VARCHAR(200),
    #     stream_tag_ids VARCHAR(200),
    #     stream_is_mature VARCHAR(200),
    #     CONSTRAINT primary_key_constraint PRIMARY KEY (stream_id)
    # )
    # """

    # cursor.execute(sql_query)
    # print("Opened database successfully")

    # try:
    #     streams_df.to_sql("top_streams", engine, index=False, if_exists='append')
    # except:
    #     print("Data already exists in the database")

    # conn.close()
    # print("Close database successfully")


    # * Get Suscriptions for a specified Broadcaster ID

    # Requires a scope (granted by the broadcaster, used in extensions)
    #broadcaster_id = streams_dict["stream_id"][0]
    #response = requests.get("https://api.twitch.tv/helix/subscriptions?broadcaster_id={broadcaster_id}", headers=headers)
    #subscriptions_data = response.json()