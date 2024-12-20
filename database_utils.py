import pandas as pd
from sqlalchemy import create_engine
import psycopg2

def fetch_data_from_sql(query, config):
    connection_string = (
        f"mssql+pyodbc://{config['user']}:{config['password']}@"
        f"{config['server']}/{config['database']}?"
        f"driver={config['driver'].replace(' ', '+')}"
    )
    engine = create_engine(connection_string)
    with engine.connect() as connection:
        df = pd.read_sql(query, connection)
    return df

def clean_dataframe(df):
    print("Limpiando datos del DataFrame...")
    for column in df.select_dtypes(include=['object']):
        df[column] = df[column].apply(lambda x: str(x).encode('utf-8', 'replace').decode('utf-8') if isinstance(x, str) else x)
    return df

def insert_data_to_postgres(df, table_name, config):
    print(f"Insertando datos en PostgreSQL: {table_name}...")
    conn = psycopg2.connect(
        host=config['host'],
        database=config['database'],
        user=config['user'],
        password=config['password']
    )
    conn.autocommit = True
    cursor = conn.cursor()

    placeholders = ', '.join(['%s'] * len(df.columns))
    columns = ', '.join(df.columns)
    query = f"INSERT INTO public.{table_name} ({columns}) VALUES ({placeholders})"

    for row in df.itertuples(index=False, name=None):
        cursor.execute(query, row)
    
    cursor.close()
    conn.close()
