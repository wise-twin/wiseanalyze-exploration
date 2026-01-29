import os
from typing import Any, Dict, List
import pandas as pd
from uuid import uuid5, UUID
from tqdm import tqdm

from dotenv import load_dotenv
load_dotenv()

UUID_NAMESPACE = UUID(os.getenv("UUID_NAMESPACE"))

def convert_to_db(df : pd.DataFrame, limit = None) -> List[Dict[str, Dict[str, Any]]]:
    """Convert ARIA accident report DataFrame to normalized database records.
    
    Processes raw ARIA accident data from CSV format into normalized records
    suitable for multi-table database insertion. Generates deterministic UUIDs
    based on site addresses and accident metadata. Parses French consequence
    strings into structured environmental/economic impact fields.
    
    Args:
        df (pd.DataFrame): Input DataFrame with required ARIA columns:
            - 'Départment': Department code/name
            - 'Commune': Municipality name
            - 'Pays': Country
            - 'Code NAF': Industrial activity classification
            - 'Titre': Accident title
            - 'Date': Accident date
            - 'Numéro ARIA': Unique ARIA incident identifier
            - 'Echelle': Severity scale
            - 'Causes profondes': Root causes
            - 'Causes premières': Immediate causes
            - 'Contenu': Accident description
            - 'Matières': Substances involved
            - 'Classe de danger CLP': CLP hazard classification
            - 'Conséquences': Consequence summary string
            - 'Type évènement': Event type/disruption duration
        limit (int, optional): Maximum number of rows to process. If None,
            processes entire DataFrame. Defaults to None.
    
    Returns:
        List[Dict[str, Dict[str, Any]]]: List of database record dictionaries,
            one per input row. Each contains the following normalized tables:
            - 'sites': Facility location and activity data
            - 'accidents': Core incident metadata with deterministic UUID
            - 'causes': Root and immediate cause classification
            - 'substances': Chemical substances involved (single entry format)
            - 'consequences_human': Human impact metrics (placeholder None values)
            - 'consequences_other': Environmental, economic impact, and disruption data
    
    Example:
        >>> df = pd.read_csv("accidents-tous-req10905.csv", encoding="cp1252", sep=";", skiprows=7)
        >>> db_records = convert_to_db(df, limit=1000)
        >>> len(db_records)
        1000
        >>> print(db_records[0]["sites"]["site_id"])  # Deterministic UUID
    """    
    if limit is not None :
        df = df.head(limit)

    def create_line(line : pd.Series):
        address = " ".join([str(line["Départment"]), str(line["Commune"])])
        site_id = str(uuid5(UUID_NAMESPACE, address))
        sites = {
            "site_id" : site_id,
            "plant_name": "",
            "address": address,
            "latitude": None,               # to fill later
            "longitude": None,              # to fill later
            "country": line["Pays"],
            "industrial_activity": line["Code NAF"],
        }

        accident_key = " ".join([str(line["Titre"]), str(line["Date"])])
        accident_id = str(uuid5(UUID_NAMESPACE, accident_key))
        accidents = {
            "accident_id": accident_id,
            "site_id": site_id,
            "title": line["Titre"],
            "source": "ARIA",
            "source_id": str(line["Numéro ARIA"]),
            "accident_date": line["Date"],
            "severity_scale": line["Echelle"],
            "raw_data": "", #line,
            "created_at": "date.now()",
            "updated_at": "",
        }

        causes = {
            "accident_id": accident_id,
            "event_category": line["Causes profondes"],
            "failure": line["Causes premières"],
            "description": line["Contenu"],
        }

        substances = {
            "substancesArray":[
                {
                    "accident_id": accident_id,
                    "name": line["Matières"],
                    "cas_number": "",
                    "quantity": "",
                    "clp_class" : line["Classe de danger CLP"]
                }
            ]
        }

        consequences_human = {
            "accident_id": accident_id,
            "fatalities": None,
            "injuries": None,
            "evacuated": None,
            "hospitalized": None,
        }

        consequences = {
            "ENVIRONNEMENTALES" : "",
            "ÉCONOMIQUES" : ""
        }

        try :
            for consequence in line["Conséquences"].split("CONSÉQUENCES "):
                if len(consequence) < 2 : continue
                s = consequence.split(',')
                key = s[0]
                content = (','.join(s[1:])).removesuffix(',')
                consequences[key] = content
        except : pass

        consequences_other = {
            "accident_id": accident_id,
            "environmental_impact": consequences["ENVIRONNEMENTALES"],
            "economic_cost": consequences["ÉCONOMIQUES"],
            "disruption_duration": line["Type évènement"]
        }

        tables = {
            "sites": sites,
            "accidents": accidents,
            "causes": causes,
            "substances": substances,
            "consequences_human": consequences_human,
            "consequences_other": consequences_other
        }
        
        return tables
    
    db_lines = []

    for x in tqdm(iter(df.iloc), total=limit, ncols=200):
        db_lines.append(create_line(x))

    return db_lines

if __name__ == "__main__":
    df = pd.read_csv("./accidents-tous-req10905.csv", encoding="cp1252", sep=";", skiprows=7)
    def print_db_jsons(json, limit=1):
        for i, db_line in enumerate(json):
            print(i, "=" * 200)
            for key in db_line :
                print(key)
                print(db_line[key])
            if i == limit-1 : break

    ARIA_db_jsons = convert_to_db(df, limit=1000)
    print_db_jsons(ARIA_db_jsons, limit=10)