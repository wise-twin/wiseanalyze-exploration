import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from parsing import convert_to_db
from utils.update_db import get_db_connection, insert_jsons_in_db
from pandas import read_csv

from dotenv import load_dotenv
from os import getenv

load_dotenv()
DB_CONN_STRING = getenv("NEON_CONNECTION_STRING")
assert DB_CONN_STRING is not None

result_df = read_csv("./accidents-tous-req10905.csv", encoding="cp1252", sep=";", skiprows=7)

db_ready_jsons = convert_to_db(result_df, limit=10)

with get_db_connection(DB_CONN_STRING, schema="WizeAnalyze") as conn :
    insert_jsons_in_db(db_ready_jsons, conn)