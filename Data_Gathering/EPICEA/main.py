import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from pandas import DataFrame
from parsing import convert_to_db
from prompts import PROMPTS, SYSTEM_MESSAGE
from langchain_openai import ChatOpenAI
from scraping import scrape, process_html_directory, CTNs
from utils.update_db import get_db_connection, insert_jsons_in_db, execute_raw_sql
from utils.Cached_LLM import Cached_LLM

from dotenv import load_dotenv
from os import getenv

load_dotenv()
OPENAI_API_KEY = getenv("OPENAI_API_KEY")
DB_CONN_STRING = getenv("NEON_CONNECTION_STRING")
assert DB_CONN_STRING is not None

llm = ChatOpenAI(
    model="gpt-5-nano",
    stream_usage=True,
    reasoning_effort="low",
    service_tier="flex"
)

def get_last_numero_dossier(conn):
    return execute_raw_sql(conn, "select MAX(source_id) from accidents where source = 'EPICEA'", fetch=True)[0][0]

CTNs_array = list(CTNs.keys())
with get_db_connection(DB_CONN_STRING, schema="WizeAnalyze") as conn :
    last_dossier = get_last_numero_dossier(conn)

last_dossier = 1 # TESTING ONLY

scraping_result_jsons = scrape(indexFrom=last_dossier, selected_CTN=CTNs_array[0])
# scraping_result_jsons = process_html_directory()

if len(scraping_result_jsons) == 0 : 
    print("Scraping got nothing new, or nothing to parse. Exiting")
    quit(1)

scraping_result_df = DataFrame(scraping_result_jsons, index=[i for i in range(len(scraping_result_jsons))])

llm = Cached_LLM(llm, SYSTEM_MESSAGE, PROMPTS)
db_ready_jsons = convert_to_db(scraping_result_df, llm)

with get_db_connection(DB_CONN_STRING, schema="WizeAnalyze") as conn :
    insert_jsons_in_db(db_ready_jsons, conn)


