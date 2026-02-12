from string import Template
from parsing import TextSchema, NumberSchema, SubstancesOutput
from langchain_core.messages import SystemMessage

PROMPTS = {
    "title" : {"prompt":Template("""
        Génère un titre concis (moins de 10 mots) qui résume l'essentiel de cette description d'accident industriel :
        $context

        Exemple :
        "Fuite propane avec périmètre sécurité 300m"

        Réponds UNIQUEMENT avec le titre, sans guillemets ni explication.
    """), "schema": TextSchema},
    "fatalities" : {"prompt":Template("""
        Extrait le nombre EXACT de morts (décès, fatalities) dans cette description d'accident :
        $context

        - Si mentionné explicitement (ex. "0 morts", "2 fatalities"), réponds ce nombre.
        - Si absent ou non chiffré (ex. "victimes", "décès possibles"), réponds 0.
        - Exemple: "Area evacuated... casualties fatalities 0" → 0
        - Exemple: "2 morts" → 2

        Réponds UNIQUEMENT avec un seul nombre entier, rien d'autre."""), 
        "schema": NumberSchema},
    "injuries" : {"prompt":Template("""
        Extrait le nombre EXACT de blessés (injuries, blessés légers/graves) dans cette description :
        $context

        - Si mentionné (ex. "3 injuries", "2 blessés légers"), réponds ce nombre.
        - Si absent ou vague (ex. "blessés non précisés"), réponds 0.
        - Exemple: "Flying debris caused minor injuries to 2 workers" → 2
        - Exemple: "Pas de blessés" → 0

        Réponds UNIQUEMENT avec un seul nombre entier."""), 
        "schema": NumberSchema},
    "evacuated" : {"prompt":Template("""
        Extrait le nombre EXACT de personnes évacuées (évacuation, confinement, périmètre sécurité avec évacuation) dans cette description :
        $context

        - Si chiffré explicitement (ex. "500 personnes évacuées", "area evacuated within 500m"), calcule/estime si rayon donné mais sinon 0.
        - Si absent ou seulement "périmètre" sans nombre, réponds 0.
        - Exemple: "Area evacuated within 500m radius" → 0 (pas de nombre précis)
        - Exemple: "Confinement riverains 2h, 100 évacués" → 100

        Réponds UNIQUEMENT avec un seul nombre entier (0 si inconnu)."""), 
        "schema": NumberSchema},
    "hospitalized" : {"prompt":Template("""
        Extrait le nombre EXACT de personnes hospitalisées (hospitalisées, admises hôpital) dans cette description :
        $context

        - Si mentionné (ex. "5 hospitalisés"), réponds ce nombre.
        - Si absent, blessures mentionnées sans hôpital, ou vague, réponds 0.
        - Exemple: "2 blessés légers, pas d'hospitalisation" → 0
        - Exemple: "3 envoyés à l'hôpital" → 3

        Réponds UNIQUEMENT avec un seul nombre entier."""), 
        "schema": NumberSchema},
    "substances" : {"prompt":Template("""
        Extrait les substances chimiques/nuisibles mentionnées explicitement dans cette description, avec quantité SI précisée :
        $context

        - Format JSON: {"substances": [{"name": "propane", "quantity": "3000L"}, ...]} ou {"substances": []} si aucune.
        - Noms exacts du texte (ex. propane, H2S, ammonia).
        - Quantité seulement si chiffrée (ex. "2000L fioul"); ignore sinon.
        - Exemple: "Fuite propane... 3000L spilled" → {"substances": [{"name": "propane", "quantity": "3000L"}]}
        - Exemple: "Pas de substance" → {"substances": []}

        Réponds UNIQUEMENT avec le JSON valide."""), 
        "schema":SubstancesOutput},
}

SYSTEM_MESSAGE = SystemMessage(content="Tu es un assistant français. Réponds UNIQUEMENT en français avec des réponses courtes et précises. Ne jamais utiliser l'anglais." \
            "Tu es un expert en extraction de données. Il va t'etre passé du text non structuré, et tu dois le convertir dans la structure donnée. " \
            "Si un nombre n'est pas mentionné, répond 0")