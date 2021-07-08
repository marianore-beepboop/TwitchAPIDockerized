import logging
import datetime
import pandas as pd
import pytz

from .extract import get_games_data, get_streams_data

def check_if_valid_data(df: pd.DataFrame) -> bool:
    # Check if dataframe is empty
    if df.empty:
        logging.info("No stream data. Finishing execution")
        return False 

    # Check for nulls
    if df.isnull().values.any():
        raise Exception("Null values found")

    return True

def check_valid_data_before_load(df: pd.DataFrame) -> bool:
    if check_if_valid_data(df):
        logging.info("Data valid, proceed to Load stage")
        return True
    else: return False

def get_games_df(data: str) -> pd.DataFrame:
    date_string = datetime.datetime.now(pytz.timezone('America/Argentina/Buenos_Aires'))
   
    game_timestamp = []
    game_id = []
    game_name = []

    for game in data["data"]:
        game_timestamp.append(date_string.strftime("%d/%m/%Y %H:%M:%S"))
        game_id.append(game["id"])
        game_name.append(game["name"])

    games_dict = {
        "game_timestamp": game_timestamp,
        "game_id": game_id,
        "game_name": game_name
    }
    
    games_df = pd.DataFrame(games_dict)
    return games_df

def get_streams_df(data: str) -> pd.DataFrame:
    date_string = datetime.datetime.now(pytz.timezone('America/Argentina/Buenos_Aires'))

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

    for stream in get_streams_data["data"]:
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
    return streams_df

