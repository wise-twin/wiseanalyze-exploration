from contextlib import contextmanager
from typing import Generator, Optional
import psycopg2, os
from psycopg2.extras import execute_values
from dotenv import load_dotenv
load_dotenv()

def insert_jsons_in_db(db_jsons : list[dict[str, dict]], conn):
    try :
        cur = conn.cursor()

        # 1. Insert sites
        # Generate an array of tuples without duplicates
        constraint_set = set()
        sites_tuples = []
        for db_json in db_jsons:
            constraint_key = db_json["sites"]["plant_name"] + db_json["sites"]["address"]
            if constraint_key in constraint_set : continue
            constraint_set.add(constraint_key)

            sites_tuples.append((
                db_json["sites"]["site_id"],
                db_json["sites"]["plant_name"], 
                db_json["sites"]["address"], 
                db_json["sites"]["latitude"], 
                db_json["sites"]["longitude"], 
                db_json["sites"]["country"], 
                db_json["sites"]["industrial_activity"]
            ))

        # Insert sites BEFORE the rest so the mappings are set
        print("Inserting sites")
        execute_values(cur, """INSERT INTO sites (site_id, plant_name, address, latitude, longitude, country, industrial_activity) VALUES %s ON CONFLICT (plant_name, address) DO NOTHING""", sites_tuples)

        # Get site mappings
        cur.execute("SELECT site_id, plant_name, address  FROM sites")
        all_sites = cur.fetchall()
        site_mapping = {(row[1], row[2]): row[0] for row in all_sites}

        # Prepare tuples for inserting
        accidents_tuples = []
        causes_tuples = []
        substances_tuples = []
        human_tuples = []
        other_tuples = []
        for db_json in db_jsons:
            accidents_tuples.append((
                db_json["accidents"]["accident_id"], 
                site_mapping[(db_json["sites"]["plant_name"], db_json["sites"]["address"])], 
                db_json["accidents"]["title"], 
                db_json["accidents"]["source"], 
                db_json["accidents"]["source_id"], 
                db_json["accidents"]["accident_date"], 
                db_json["accidents"]["severity_scale"]
            )) 

            causes_tuples.append((
                db_json["causes"]["accident_id"], 
                db_json["causes"]["event_category"], 
                db_json["causes"]["failure"], 
                db_json["causes"]["description"]
            ))

            for substance in db_json["substances"]["substancesArray"]:
                substances_tuples.append((
                    substance["accident_id"], 
                    substance["name"], 
                    substance["cas_number"], 
                    substance["quantity"], 
                    substance["clp_class"]
                ))

            human_tuples.append((
                db_json["consequences_human"]["accident_id"], 
                db_json["consequences_human"]["fatalities"], 
                db_json["consequences_human"]["injuries"], 
                db_json["consequences_human"]["evacuated"], 
                db_json["consequences_human"]["hospitalized"]
            ))

            other_tuples.append((
                db_json["consequences_other"]["accident_id"], 
                db_json["consequences_other"]["environmental_impact"], 
                db_json["consequences_other"]["economic_cost"], 
                db_json["consequences_other"]["disruption_duration"]
            ))

        print("Inserting accidents")
        execute_values(cur, """INSERT INTO accidents (accident_id, site_id, title, source, source_id, accident_date, severity_scale) VALUES %s ON CONFLICT DO NOTHING""", accidents_tuples)

        print("Inserting causes")
        execute_values(cur, """INSERT INTO causes (accident_id, event_category, failure, description) VALUES %s ON CONFLICT (accident_id) DO NOTHING""", causes_tuples)

        print("Inserting substances")
        execute_values(cur, """INSERT INTO substances (accident_id, name, cas_number, quantity, clp_class) VALUES %s ON CONFLICT (accident_id) DO NOTHING""", substances_tuples)

        print("Inserting consequences_human")
        execute_values(cur, """INSERT INTO consequences_human (accident_id, fatalities, injuries, evacuated, hospitalized) VALUES %s ON CONFLICT (accident_id) DO NOTHING""", human_tuples)

        print("Inserting consequences_other")
        execute_values(cur, """INSERT INTO consequences_other (accident_id, environmental_impact, economic_cost, disruption_duration) VALUES %s ON CONFLICT (accident_id) DO NOTHING""", other_tuples)

        # Commit all inserts
        conn.commit()
    finally :
        cur.close()

@contextmanager
def get_db_connection(connection_string: str, schema: Optional[str] = None) -> Generator:
    """Context manager for PostgreSQL database connections.
    
    Provides automatic connection cleanup and error handling using context manager
    pattern. Ensures proper resource management and connection closure.
    
    Args:
        connection_string (str): PostgreSQL connection string.
    
    Yields:
        psycopg2.connection: Active database connection.
    
    Raises:
        psycopg2.Error: Any database-related errors.
    
    Example:
        >>> with get_db_connection("postgresql://user:pass@localhost/db") as conn:
        ...     cursor = conn.cursor()
        ...     cursor.execute("SELECT * FROM users")
    """
    conn = None
    try:
        conn = psycopg2.connect(connection_string)
        if schema is not None:
            cursor = conn.cursor()
            cursor.execute(f'SET search_path TO "{schema}"')
            cursor.close()
        yield conn
        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Database operation failed: {e}")
        raise
    finally:
        if conn:
            conn.close()
            print("Database connection closed")

def execute_raw_sql(conn, query: str, fetch: bool = False):
    """Execute raw SQL query with optional result fetching.
    
    Low-level function for executing arbitrary SQL. Use with caution and
    only with trusted SQL strings (parameterized queries preferred).
    
    Args:
        conn : PostgreSQL connection.
        query (str): Raw SQL query string.
        fetch (bool): If True, fetch and return all results. Defaults to False.
    
    Returns:
        Any: Query results if fetch=True, otherwise rows affected count.
    
    Raises:
        psycopg2.Error: If query execution fails.
    
    Warning:
        Only use this function with hardcoded or fully validated SQL strings.
        For dynamic queries, use parameterized queries with the other functions.
    
    Example:
        >>> result = execute_raw_sql(
        ...     "postgresql://user:pass@localhost/db",
        ...     "SELECT version()",
        ...     fetch=True
        ... )
    """
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        if fetch:
            results = cursor.fetchall()
            print(f"Raw query returned {len(results)} rows")
            return results
        else:
            rows_affected = cursor.rowcount
            print(f"Raw query affected {rows_affected} rows")
            return rows_affected
    finally:
        cursor.close()

if __name__ == "__main__":
    conn_string = os.getenv("NEON_CONNECTION_STRING")
    assert conn_string is not None
    result = execute_raw_sql(conn_string, "select MAX(source_id) from accidents where source = 'EPICEA'", fetch=True)[0][0]
    print(result)