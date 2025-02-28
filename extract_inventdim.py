from config import SQL_SERVER_CONFIG, POSTGRES_CONFIG
from database_utils import fetch_data_from_sql, clean_dataframe
import logging
from sqlalchemy import create_engine, text
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def get_last_recid_and_modifieddatetime_from_postgres(config):
    """
    Obtiene el último RECID y MODIFIEDDATETIME insertado en PostgreSQL.
    """
    try:
        logging.info("Obteniendo el último RECID y MODIFIEDDATETIME de PostgreSQL...")
        connection_string = (
            f"postgresql+psycopg2://{config['user']}:{config['password']}@"
            f"{config['host']}/{config['database']}"
        )
        engine = create_engine(connection_string)
        with engine.connect() as connection:
            query = text("SELECT MAX(recid), MAX(modifieddatetime) FROM inventdim;")
            result = connection.execute(query).fetchone()
            last_recid, last_modifieddatetime = result
        
        if last_recid is None:
            logging.info("No hay registros en la tabla inventdim. Se insertarán todos los datos.")
            return None, None
        else:
            logging.info(f"Último RECID en PostgreSQL: {last_recid}")
            logging.info(f"Último MODIFIEDDATETIME en PostgreSQL: {last_modifieddatetime}")
            return last_recid, last_modifieddatetime
    except Exception as e:
        logging.error(f"Error al obtener el último RECID y MODIFIEDDATETIME: {e}")
        raise

def upsert_data_to_postgres(df, table_name, config):
    """
    Realiza un upsert (insertar o actualizar) en PostgreSQL usando INSERT ... ON CONFLICT.
    """
    try:
        logging.info(f"Realizando upsert en PostgreSQL: {table_name}...")
        connection_string = (
            f"postgresql+psycopg2://{config['user']}:{config['password']}@"
            f"{config['host']}/{config['database']}"
        )
        engine = create_engine(connection_string)

        # Convertir los nombres de las columnas a minúsculas
        df.columns = df.columns.str.lower()

        with engine.begin() as connection:
            for _, row in df.iterrows():
                columns = ', '.join(df.columns)
                placeholders = ', '.join([f":{col}" for col in df.columns])
                update_assignments = ', '.join([f"{col}=EXCLUDED.{col}" for col in df.columns if col != "recid"])
                upsert_query = text(f"""
                    INSERT INTO {table_name} ({columns})
                    VALUES ({placeholders})
                    ON CONFLICT (recid)
                    DO UPDATE SET {update_assignments};
                """)
                #print(upsert_query)
                connection.execute(upsert_query, row.to_dict())

        logging.info(f"Upsert completado en {table_name}.")
    except Exception as e:
        logging.error(f"Error en el upsert: {e}")
        raise

def process_inventdim():
    try:
        logging.info("Iniciando extracción incremental de INVENTDIM...")

        # Obtener el último RECID y MODIFIEDDATETIME insertado en PostgreSQL
        last_recid, last_modifieddatetime = get_last_recid_and_modifieddatetime_from_postgres(POSTGRES_CONFIG)

        # Construir la consulta SQL para extraer registros nuevos y modificados
        query = """
        SELECT  
        INVENTDIMID, INVENTLOCATIONID, MODIFIEDDATETIME, CREATEDDATETIME, RECID, DATAAREAID
        FROM INVENTDIM 
        """
        if last_recid:
            query += f" WHERE (RECID > {last_recid} OR MODIFIEDDATETIME > '{last_modifieddatetime}')"
        
        logging.info(f"Consulta SQL para extracción incremental: {query}")

        # Extraer datos de SQL Server
        df = fetch_data_from_sql(query, SQL_SERVER_CONFIG)
        if df.empty:
            logging.warning("No hay nuevos registros para insertar o actualizar.")
            return

        # Limpiar y cargar datos en PostgreSQL
        df = clean_dataframe(df)
        upsert_data_to_postgres(df, "inventdim", POSTGRES_CONFIG)
        logging.info("Proceso de carga incremental completado.")
    except Exception as e:
        logging.error(f"Error en el proceso de INVENTDIM: {e}")
        raise