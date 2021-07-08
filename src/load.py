import logging
import sqlalchemy
import pandas as pd
from .transform import check_valid_data

DATABASE_LOCATION = "postgresql://postgres:postgres@postgres_data/db_data"

def df_to_sql(df: pd.DataFrame, db_name: str):
    if check_valid_data(df):
        engine = sqlalchemy.create_engine(DATABASE_LOCATION)
        df.to_sql(f"{db_name}", engine, index=True, if_exists="append")
        logging.info(f"Data from {db_name} loaded into DB")


df_to_sql(games_df, 'top_games')
df_to_sql(streams_df, 'top_streams')