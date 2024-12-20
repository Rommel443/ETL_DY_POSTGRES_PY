from config import SQL_SERVER_CONFIG, POSTGRES_CONFIG
from database_utils import fetch_data_from_sql, clean_dataframe, insert_data_to_postgres

def process_inventdim():
    query = """
    SELECT top 10 INVENTDIMID, INVENTLOCATIONID, MODIFIEDDATETIME, CREATEDDATETIME, RECID 
    FROM INVENTDIM
    """
    df = fetch_data_from_sql(query, SQL_SERVER_CONFIG)
    df = clean_dataframe(df)
    insert_data_to_postgres(df, "inventdim", POSTGRES_CONFIG)
