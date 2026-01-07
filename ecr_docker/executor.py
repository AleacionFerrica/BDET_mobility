
import duckdb
import os
import argparse
import base64

def get_db_connection():
    # 1. Recuperar credenciales desde Variables de Entorno (Inyectadas por AWS Batch/Airflow)
    aws_access_key = os.environ.get('AWS_ACCESS_KEY_ID')
    aws_secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
    aws_region = os.environ.get('AWS_DEFAULT_REGION', 'eu-central-1')
    dt_path = os.environ.get("DATA_PATH")
    # Credenciales de Neon/Postgres
    pg_host = os.environ.get('PG_HOST')
    pg_port = os.environ.get('PG_PORT', '5432')
    pg_db = os.environ.get('PG_DATABASE')
    pg_user = os.environ.get('PG_USER')
    pg_pass = os.environ.get('PG_PASSWORD')

    print("🔌 Iniciando conexión DuckDB...")
    con = duckdb.connect()

    # 2. Instalar extensiones
    # NOTA: 'ducklake' debe estar disponible en el repositorio de extensiones
    # o venir preinstalada en la imagen.
    con.sql("INSTALL ducklake; LOAD ducklake;")
    con.sql("INSTALL spatial; LOAD spatial;")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.sql("INSTALL postgres; LOAD postgres;")

    # 3. Crear Secreto S3
    con.execute(f"""
        CREATE OR REPLACE SECRET secreto_s3 (
            TYPE S3,
            KEY_ID '{aws_access_key}',
            SECRET '{aws_secret_key}',
            REGION '{aws_region}'
        );
    """)

    # 4. Crear Secreto Postgres
    con.execute(f"""
        CREATE OR REPLACE SECRET secreto_postgres (
            TYPE postgres,
            HOST '{pg_host}',
            PORT {pg_port},
            DATABASE '{pg_db}',
            USER '{pg_user}',
            PASSWORD '{pg_pass}'
        );
    """)

    # 5. Crear Secreto DuckLake y Attach
    con.execute("""
        CREATE OR REPLACE SECRET secreto_ducklake (
            TYPE ducklake,
            METADATA_PATH '',
            METADATA_PARAMETERS MAP {'TYPE': 'postgres', 'SECRET': 'secreto_postgres'}
        );
    """)

    con.execute(f"""
        ATTACH 'ducklake:secreto_ducklake' AS mobility_ducklake (DATA_PATH '{dt_path}') 
    """)
    
    con.execute("USE mobility_ducklake")
    
    return con

def run_job(encoded_query):
    try:
        # Decodificar la query que viene de Airflow
        query = base64.b64decode(encoded_query).decode('utf-8')
        print(f"Ejecutando Query en Batch:\n{query}")

        con = get_db_connection()
        con.begin()
        # Ejecutar la query
        con.sql(query)
        print("Ejecución exitosa")
        con.commit()
        
    except Exception as e:
        con.rollback()
        print(f" Error crítico: {e}")
        raise e
    finally:
        try:
            con.close()
        except:
            pass

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--query_b64", help="SQL Query codificada en Base64", required=True)
    args = parser.parse_args()
    
    run_job(args.query_b64)