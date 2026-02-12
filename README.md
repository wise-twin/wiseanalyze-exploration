# Industrial Accident ETL (ARIA & EPICEA → Postgres)
This repository contains a small ETL pipeline that ingests French industrial accident data from the ARIA and EPICEA sources, transforms it into a normalized JSON structure, and loads it into a PostgreSQL database.

## Project structure
```text
N:.
│   README.md
│
├── ARIA
│   ├── main.py
│   ├── parsing.py
│   └── __init__.py
│
├── EPICEA
│   ├── main.py
│   ├── parsing.py
│   ├── scraping.py
│   └── __init__.py
│
└── utils
    ├── Cached_LLM.py
    ├── update_db.py
    └── __init__.py
```
- /ARIA – Ingestion and transformation for ARIA CSV exports (Ministère de l’Environnement). [file:1][file:2]

- /EPICEA – Web scraping and transformation for EPICEA accident sheets (INRS). [file:3][file:4][file:5]

- /utils – Shared utilities: PostgreSQL access and LLM caching. [file:6][file:7]

# ARIA pipeline
## Source
The ARIA pipeline expects a CSV export such as accidents-tous-req10905.csv (encoding cp1252, separator ;, 7 header lines to skip).

## Transformation
```ARIA/parsing.py``` exposes convert_to_db(df, limit=None) which:

- Builds deterministic site_id UUIDs from department and commune.
- Builds deterministic accident_id UUIDs from title and date.
- Normalizes each row into six logical tables:
  - sites
  - accidents
  - causes
  - substances (single entry per accident)
  - consequences_human (placeholders)
  - consequences_other (parsed from the free-text “Conséquences” field)

The function returns a list of JSON-like dicts:

```python
List[Dict[str, Dict[str, Any]]]
```

## Load
```ARIA/main.py``` demonstrates the full flow:

- Read the ARIA CSV into a pandas DataFrame.
- Call convert_to_db(result_df, limit=10).
- Open a Postgres connection with get_db_connection.
- Insert into the database with insert_jsons_in_db.

# EPICEA pipeline
## Scraping
```EPICEA/scraping.py``` scrapes the INRS EPICEA public interface.

Key points:
- Uses Selenium + Chrome WebDriver and BeautifulSoup.
- Filters accidents by:
  - minimal dossier number (indexFrom)
  - CTN (industry classification) from the CTNs dict.
  - For each record, extracts fields from the HTML table into a dict (e.g. Numéro du dossier, Date de l'accident, Comité technique national, Résumé de l'accident, etc.).

Two entry points:
- ```scrape(indexFrom=1, selected_CTN="D - Services, commerces et industries de l'alimentation", limit=2, write_html_on_disk=False)```
- ```process_html_directory(directory_path="./epicea_results", extension="*.html")``` to re-parse previously dumped HTML files.

## LLM-assisted parsing
```EPICEA/parsing.py``` turns scraped EPICEA records into normalized DB-ready JSON.

It defines:
- Pydantic models:
  - ```NumberSchema``` for scalar integer outputs (fatalities, injuries, etc.).
  - ```TextSchema``` for short text answers such as titles.
  - ```Substance``` and ```SubstancesOutput``` for chemicals involved.

- ```convert_to_db(df, llm, trunc=None)``` which:
  - Builds deterministic ```site_id``` and ```accident_id``` UUIDs from a UUID namespace.
  - Uses a Cached_LLM instance to:
    - generate a concise French title from ```Résumé de l'accident```
    - extract casualty numbers
    - extract a list of substances with CAS, quantity and CLP class.
  - Produces the same logical table structure as the ARIA pipeline (```sites```, ```accidents```, ```causes```, ```substances```, ```consequences_human```, ```consequences_other```).

A typical row uses:
- ```Comité technique national``` → ```industrial_activity```
- ```Code entreprise``` → cause category
- ```Matériel en cause``` → failure / equipment
- ```Résumé de l'accident``` → description and LLM context.

## Orchestration
```EPICEA/main.py``` runs the EPICEA pipeline end-to-end.

Steps:

1. Compute ```last_dossier``` using ```execute_raw_sql("select MAX(source_id) from accidents where source = 'EPICEA'")```.
2. Build the CTN list from ```scraping.CTNs``` and pick the first CTN.
3. Call ```scrape(indexFrom=last_dossier, selected_CTN=CTNs_array[0])```.
4. Wrap the base ```ChatOpenAI``` client into ```Cached_LLM``` with:
    - French-only system message.
    - Prompt templates for title, fatalities, injuries, evacuated, hospitalized, substances.
5. Run ```convert_to_db(scraping_result_df, llm)```.
6. Insert all resulting JSONs with insert_jsons_in_db. [file:4][file:6]

# Utilities
## Database utilities
```utils/update_db.py``` provides:

- ```get_db_connection(connection_string, schema=None)``` – context manager around psycopg2 with automatic ```SET search_path``` and commit/rollback.

- ```insert_jsons_in_db(db_jsons, conn)``` – bulk inserts into:
  - sites
  - accidents
  - causes
  - substances
  - consequences_human
  - consequences_other using ```psycopg2.extras.execute_values``` and simple conflict handling (```ON CONFLICT DO NOTHING``` or on ```(plant_name, address)```).

- ```execute_raw_sql(conn, query, fetch=False)``` – helper for ad-hoc queries.

The insert function deduplicates sites by ```(plant_name, address)``` and builds a mapping from ```(plant_name, address)``` to ```site_id``` before inserting accidents.

## LLM caching
```utils/Cached_LLM.py``` wraps a LangChain ChatOpenAI instance.

Features:
- JSON file cache keyed by MD5 hash of the full prompt context.
- ```prompt(context, schema, force_run=False)```:
  - returns a Pydantic model instance, either from cache or by calling the LLM with ```with_structured_output(schema, include_raw=True)```.
- ```ask_ai(field, context)```:
  - picks a pre-configured prompt + schema from ```prompts[field]```,
  - substitutes the context into the prompt,
  - returns the ```.response``` field of the parsed Pydantic output.

# Environment and configuration
The project relies on environment variables loaded via python-dotenv.

Required:
- ```NEON_CONNECTION_STRING``` – PostgreSQL connection string (Neon or compatible).
- ```UUID_NAMESPACE``` – UUID namespace (string) used to generate deterministic IDs.
- ```OPENAI_API_KEY``` – for ChatOpenAI.
- Optional: typical Selenium/ChromeDriver setup in your PATH.

# How to run
1. Install dependencies
Using pip (example):

```bash
pip install pandas psycopg2-binary python-dotenv selenium beautifulsoup4 tqdm langchain-openai langchain-core pydantic
```
Make sure chromedriver is installed and compatible with your Chrome version for the EPICEA scraper.

2. Prepare ```.env```
Create a ```.env``` file at the repo root:
```
text
NEON_CONNECTION_STRING=postgresql://user:password@host:port/dbname
UUID_NAMESPACE=00000000-0000-0000-0000-000000000000
OPENAI_API_KEY=sk-...
```
## Run ARIA pipeline
Place your ARIA CSV (e.g. ```accidents-tous-req10905.csv```) at the repo root, then:

```bash
cd ARIA
python main.py
```
This will:
- Read the CSV.
- Transform the first 10 rows into DB-ready JSON.
- Insert into the configured database schema (WizeAnalyze by default).

## Run EPICEA pipeline
To scrape new EPICEA records and insert them:

```bash
cd EPICEA
python main.py
```

This will:
- Query the database for the last imported EPICEA dossier number.
- Scrape EPICEA from that ID for the first CTN.
- Use the LLM to extract title, casualties and substances.
- Insert normalized records into the same PostgreSQL schema.

For offline parsing of saved HTML:

```python
from scraping import process_html_directory
from parsing import convert_to_db
from utils.Cached_LLM import Cached_LLM
```
and then feed the resulting DataFrame into ```convert_to_db```.

# Notes and limitations
- The EPICEA parser relies on the current HTML structure (```table.tablein[2]```) and may break if INRS updates the site.
- LLM extraction assumes French-language descriptions and returns French outputs.
- Schema and table names (sites, accidents, etc.) must exist in your PostgreSQL database with compatible columns.