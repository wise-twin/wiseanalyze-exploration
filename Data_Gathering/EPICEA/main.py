import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from string import Template
from pandas import DataFrame
from parsing import convert_to_db
from langchain_openai import ChatOpenAI
from langchain_core.messages import SystemMessage
from scraping import scrape, process_html_directory, CTNs
from parsing import TextSchema, NumberSchema, SubstancesOutput
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

PROMPTS = {
    "title" : {"prompt":Template("J'ai besoin d'un titre qui résume en une petite phrase cette description:\n$context"), "schema": TextSchema},
    "fatalities" : {"prompt":Template("Combien de morts y a t il dans la description suivante:\n$context\n\n\nNe répond qu'un seul nombre"), "schema": NumberSchema},
    "injuries" : {"prompt":Template("Combien de blessés y a t il dans la description suivante:\n$context\n\n\nNe répond qu'un seul nombre"), "schema": NumberSchema},
    "evacuated" : {"prompt":Template("Combien de personnes évacuées y a t il dans la description suivante:\n$context\n\n\nNe répond qu'un seul nombre"), "schema": NumberSchema},
    "hospitalized" : {"prompt":Template("Combien de personnes hospitalisées y a t il dans la description suivante:\n$context\n\n\nNe répond qu'un seul nombre"), "schema": NumberSchema},
    "substances" : {"prompt":Template("Quelles substances sont en jeu dans la description suivante:\n$context\n\n\nS'il n'y en a pas répond un JSON vide. Si la quantité n'est pas renseignée, ne met rien"), "schema":SubstancesOutput},
}

SYSTEM_MESSAGE = SystemMessage(content="Tu es un assistant français. Réponds UNIQUEMENT en français avec des réponses courtes et précises. Ne jamais utiliser l'anglais." \
            "Tu es un expert en extraction de données. Il va t'etre passé du text non structuré, et tu dois le convertir dans la structure donnée. " \
            "Si un nombre n'est pas mentionné, répond 0")

def get_last_numero_dossier(conn):
    return execute_raw_sql(conn, "select MAX(source_id) from accidents where source = 'EPICEA'", fetch=True)[0][0]

CTNs_array = list(CTNs.keys())
with get_db_connection(DB_CONN_STRING, schema="WizeAnalyze") as conn :
    last_dossier = get_last_numero_dossier(conn)

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


