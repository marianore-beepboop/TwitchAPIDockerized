import datetime, pytz
import sqlalchemy
import pandas as pd


def run_twitch_info():
    DATABASE_LOCATION = "postgresql://postgres:postgres@postgres_data/db_data"
    engine = sqlalchemy.create_engine(DATABASE_LOCATION)

    result_set = engine.execute("SELECT * FROM top_games")

    for r in result_set:
        print(r)
