import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.Cached_LLM import Cached_LLM

import os
import pandas as pd
from langchain_core.messages import SystemMessage, HumanMessage
from langchain_openai import ChatOpenAI
from pydantic import BaseModel
from pydantic import Field
from uuid import uuid5, UUID
from string import Template
from typing import List
from tqdm import tqdm

from dotenv import load_dotenv
load_dotenv()

UUID_NAMESPACE = UUID(os.getenv("UUID_NAMESPACE"))

class NumberSchema(BaseModel):
    response:int

class TextSchema(BaseModel):
    response:str

class Substance(BaseModel):
    """Single chemical substance involved in incident."""
    name: str = Field(..., description="Chemical name")
    cas_number: str = Field(..., description="CAS registry number")
    quantity: str = Field(..., description="Quantity released/spilled")
    clp_class: str = Field(..., description="CLP hazard classification")

class SubstancesOutput(BaseModel):
    """Extracted substances from accident report (0 to many)."""
    response: List[Substance] = Field(default_factory=list, description="List of substances")

def convert_to_db(df : pd.DataFrame, llm : Cached_LLM, trunc : None | int = None) -> list[dict[str, dict]]:
    """Convert industrial accident report dataframe to normalized database records.
    
    Processes raw EPICEA accident report data and transforms it into normalized
    database records across multiple tables (sites, accidents, causes, substances,
    consequences). Uses LLM-powered extraction to parse unstructured accident
    descriptions and extract structured data elements.
    
    Args:
        df (pd.DataFrame): Input dataframe containing accident reports with columns:
            - 'Numéro du dossier': Unique incident identifier
            - 'Comité technique national': Industry classification code
            - 'Code entreprise': Enterprise/equipment classification code
            - 'Matériel en cause': Equipment involved in accident
            - 'Résumé de l'accident': Unstructured accident description text
        llm (Cached_LLM): Cached LLM instance for structured data extraction.
        trunc (int, optional): Maximum number of rows to process. If None,
            processes all rows. Defaults to None.
    
    Returns:
        List[dict]: List of database record dictionaries, one per input row.
            Each contains the following table entries:
            - 'sites': Facility information (plant_name, address, coordinates, etc.)
            - 'accidents': Core incident metadata (title, source, date, etc.)
            - 'causes': Incident causation and equipment failure data
            - 'substances': List of involved chemical substances
            - 'consequences_human': Casualty statistics (fatalities, injuries, etc.)
            - 'consequences_other': Environmental and economic impact data
    
    Raises:
        ValueError: If required columns are missing from input dataframe.
    
    Example:
        >>> df = pd.read_csv('epicea_accidents.csv')
        >>> llm = Cached_LLM(ChatOpenAI(...), SYSTEM_MESSAGE, PROMPTS)
        >>> db_records = convert_to_db(df, llm, trunc=100)
        >>> len(db_records)
        100
    """
    if trunc is not None :
        df = df.head(trunc)

    def create_line(line : pd.Series):
        address = "NULL"
        site_id = str(uuid5(UUID_NAMESPACE, address))
        sites = {
            "site_id" : site_id,
            "plant_name": "NULL",
            "address": address,
            "latitude": None,
            "longitude": None,
            "country": "France",
            "industrial_activity": line["Comité technique national"].split(' - ')[1],
        }

        CONTEXT_FOR_AI = line["Résumé de l'accident"]
        title : str = llm.ask_ai("title", CONTEXT_FOR_AI)

        accident_key = " ".join([title, str(line["Numéro du dossier"])])
        accident_id = str(uuid5(UUID_NAMESPACE, accident_key))
        accidents = {
            "accident_id": accident_id,
            "site_id": site_id,
            "title": title,
            "source": "EPICEA",
            "source_id": str(line["Numéro du dossier"]),
            "accident_date": None,
            "severity_scale": None,
            "raw_data": "", #line,
            "updated_at": "",
        }

        causes = {
            "accident_id": accident_id,
            "event_category": line["Code entreprise"].split(' - ')[1],
            "failure": line["Matériel en cause"],
            "description": line["Résumé de l'accident"], 
        }

        substancesOutput : List[Substance] = llm.ask_ai("substances", CONTEXT_FOR_AI) # type: ignore
        substancesArray = []
        for substance in substancesOutput :
            substanceJSON = {
                "accident_id": accident_id,
                "name":substance.name,
                "cas_number":substance.cas_number,
                "quantity":substance.quantity,
                "clp_class":substance.clp_class
            }
            substancesArray.append(substanceJSON)
        substances = {
            "substancesArray":substancesArray
        }

        consequences_human = {
            "accident_id": accident_id,
            "fatalities": llm.ask_ai("fatalities", CONTEXT_FOR_AI),
            "injuries": llm.ask_ai("injuries", CONTEXT_FOR_AI),
            "evacuated": llm.ask_ai("evacuated", CONTEXT_FOR_AI),
            "hospitalized": llm.ask_ai("hospitalized", CONTEXT_FOR_AI),
        }

        consequences_other = {
            "accident_id": accident_id,
            "environmental_impact": llm.ask_ai("environmental_impact", CONTEXT_FOR_AI),
            "economic_cost": llm.ask_ai("economic_cost", CONTEXT_FOR_AI),
            "disruption_duration": llm.ask_ai("disruption_duration", CONTEXT_FOR_AI)
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

    for x in tqdm(iter(df.iloc), total=trunc, ncols=200):
        db_lines.append(create_line(x))

    return db_lines



if __name__ == "__main__":
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
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

    llm = Cached_LLM(llm, SYSTEM_MESSAGE, PROMPTS)

    context = "Après l'intervention, le bilan a été 1 mort, deux évacués et 7 hospitalisations"
    result = llm.prompt(f"Extrait le nombre d'accidentés de ceci:\n\n{context}", NumberSchema)
    print(result)

    epicea_example = {'Numéro du dossier': '27709', 'Comité technique national': "D - Services, commerces et industries de l'alimentation", 'Code entreprise': "1091Z - Fabrication d'aliments pour animaux de ferme", 'Matériel en cause': "510308 - Autre type d'échelle", "Résumé de l'accident": "Un technicien de maintenance de 64 ans, accompagné du responsable maintenance, intervient pour réparer le capotage du convoyeur à bande. L'opération consiste à souder une plaque métallique de 30 x 30 cm afin de remplacer une partie endommagée. La plateforme individuelle roulante légère (PIRL) disponible étant insuffisamment haute pour accéder à la zone de travail, une échelle mobile deux pans de grande dimension est utilisée. Elle est déployée sur une longueur de 5 mètres.\nLe technicien installe et fixe l'échelle : il la positionne presque à la verticale, en appui sur la plateforme supérieure en caillebotis, puis sécurisée à l'aide d'une corde attachée à la rambarde, côté droit.\nPendant ce temps, le responsable maintenance monte sur la plateforme pour y déposer le matériel nécessaire à l'intervention : rallonge électrique, poste à souder, seau, baguette de soudure, marteau, masque, gants etc. Il reste sur place pour transmettre les outils à son collègue.\nL'intervention débute : la plaque est soudée en deux points sur son côté droit. A 15 h 50, le responsable récupère les équipements de protection individuelle et les outils tenus par le technicien puis se rend à l'atelier pour découper un fer plat, indispensable à la poursuite de la réparation. Le technicien reste seul sur l'échelle en attendant son retour.\nA son retour sur le lieu de l'intervention, le responsable découvre le technicien allongé au pied de l'échelle, inconscient, couché sur le flanc droit, sans aucune plaie apparente. Il respire de manière forte et bruyante.\nLes collègues interviennent immédiatement et le placent en position latérale de sécurité (PLS). Les secours sont appelés sans délai et arrivent à 16 h 10. Le technicien a repris connaissance quelques instants avant l’arrivée des secours et est évacué. Il est décédé ultérieurement.\nLors des échanges avec le médecin du Samu, il aurait dit ne pas être tombé de l’échelle. En l’absence de caméra à cet endroit, il est difficile de pouvoir comprendre le déroulé des faits et il n'y a aucune certitude que le technicien se soit cogné, ait chuté ou fait un malaise. Le médecin du Samu qui l’a pris en charge a évoqué l’hypothèse d’un malaise cardiaque. D’après ses collègues, il semblait souffrir d’hypertension artérielle."}

    df = pd.DataFrame(epicea_example, index=[0])
    EPICEA_db_jsons = convert_to_db(df, llm)