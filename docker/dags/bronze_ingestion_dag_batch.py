from airflow.sdk import Asset, dag, task
from airflow.sdk.bases.hook import BaseHook
from airflow.providers.amazon.aws.operators.batch import BatchOperator
import duckdb
import pandas as pd
import requests
import os
import re
import tempfile
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from pathlib import Path

# --- CONFIGURACIÓN ---
# En Astro/Docker, 'include' suele usarse para archivos persistentes o datos locales
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/usr/local/airflow")
DB_PATH = "include/mobility.ducklake"
pg = BaseHook.get_connection("neon_postgres")
aws = BaseHook.get_connection("aws_default")
# Función auxiliar para conectar a DuckDB con las extensiones necesarias
def get_db_connection():
    pg = BaseHook.get_connection("neon_postgres")
    aws = BaseHook.get_connection("aws_default")
    con = duckdb.connect( )
    con.sql("INSTALL ducklake; LOAD ducklake;")
    con.sql("INSTALL spatial; LOAD spatial;")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.sql("INSTALL postgres; LOAD postgres;")

    con.execute(f"""
        CREATE OR REPLACE SECRET secreto_s3 (
        TYPE s3,
        KEY_ID '{aws.login}',
        SECRET '{aws.password}',
        REGION 'eu-central-1'
    )
    """)

    con.execute(f"""
        CREATE OR REPLACE SECRET secreto_postgres (
        TYPE postgres,
        HOST '{pg.host}',
        PORT {pg.port},
        DATABASE '{pg.schema}',
        USER '{pg.login}',
        PASSWORD '{pg.password}'
        )
    """)
    con.execute("""
        CREATE OR REPLACE SECRET secreto_ducklake (
            TYPE ducklake,
            METADATA_PATH '',
            METADATA_PARAMETERS MAP {'TYPE': 'postgres', 'SECRET': 'secreto_postgres'}
        );
        """)
    con.execute("""
        ATTACH 'ducklake:secreto_ducklake' AS mobility_ducklake (DATA_PATH 's3://yena-s3-ducklake') """)
    con.execute("""
        USE mobility_ducklake """)

    
    return con

@dag(
    dag_id='bronze_ingesta_batch',
    #schedule_interval='@weekly', # Ejecutar semanalmente o cuando quieras
    #start_date=days_ago(1),
    default_args={'owner': 'airflow','retries': 3,'retry_delay': timedelta(minutes=1)},

    
    catchup=False,
    max_active_tasks=28,
    tags=['master', 'duckdb', 'mitma', 'bronze']
)
def mobility_dag():

    @task()
    def init_schema():
        """Inicializa la base de datos y el esquema si no existen"""
        # Solo necesitamos abrir la conexión para que se ejecuten los comandos del helper
        con = get_db_connection()
        con.sql("CREATE SCHEMA IF NOT EXISTS bronze;")
        con.close()


    @task()
    def ingest_catalog():
        """Descarga y actualiza el catálogo de MITMA"""
        catalog_URL = "https://movilidad-opendata.mitma.es/RSS.xml"
        response = requests.get(catalog_URL)
        response.raise_for_status()

        root = ET.fromstring(response.content)
        items = []
        
        # ... (Tu lógica de parsing original) ...
        for item in root.findall('./channel/item'):
            title = item.find('title').text.strip() if item.find('title') is not None else ""
            link = item.find('link').text.strip() if item.find('link') is not None else ""
            pub_date_raw = item.find('pubDate').text.strip() if item.find('pubDate') is not None else ""
            
            try:
                pub_date = datetime.strptime(pub_date_raw, "%a, %d %b %Y %H:%M:%S %Z")
            except ValueError:
                pub_date = None

            filename = link.split('/')[-1]
            lower_link = link.lower()
            lower_filename = filename.lower()
            
            # A) Categoría Principal (estudios_completos, estudios_basicos, etc.)
            main_category = "Otros"
            if "estudios_completos" in lower_link:
                main_category = "Estudios Completos"
            elif "estudios_basicos" in lower_link:
                main_category = "Estudios Basicos"
            elif "estudios_rutas" in lower_link:
                main_category = "Estudios de Rutas"
            elif "zonificacion" in lower_link:
                main_category = "Zonificacion"
            
            # B) Tipo de Estudio (viajes, etapas, etc.)
            study_type = "Desconocido"
            if "viajes" in lower_filename:
                study_type = "Viajes"
            elif "etapas" in lower_filename:
                if "_c" in lower_filename or "carretera" in lower_link:
                    study_type = "Etapas (Carretera)"
                else:
                    study_type = "Etapas"
            elif "pernoctaciones" in lower_filename:
                study_type = "Pernoctaciones"
            elif "personas" in lower_filename:
                study_type = "Personas"
            elif "frecuencia" in lower_link or "frecuencia" in lower_filename:
                study_type = "Frecuencia"
            elif "calidad" in lower_link or "descartados" in lower_filename:
                study_type = "Calidad"
            elif "od_rutas" in lower_filename:
                study_type = "Matriz OD Rutas"
            elif "relaciones_tramos" in lower_filename:
                study_type = "Relaciones Tramos-Rutas"
            elif "tramos_info" in lower_filename:
                study_type = "Info Tramos OD"
            elif "zonificacion" in lower_link:
                study_type = "Geometria/Zonificacion"
            elif "agregados" in lower_filename:
                study_type = "Datos Agregados"
            
            # C) Zona (municipios, distritos, GAU)
            zone_type = "N/A" # Por defecto
            if "municipios" in lower_link or "municipios" in lower_filename:
                zone_type = "Municipios"
            elif "distritos" in lower_link or "distritos" in lower_filename:
                zone_type = "Distritos"
            elif "gau" in lower_link or "gau" in lower_filename:
                zone_type = "GAU"
            elif "rutas" in lower_link:
                zone_type = "Rutas"
            
            year, month, day = None, None, None
            date_match_daily = re.search(r'(\d{4})(\d{2})(\d{2})', filename)
            date_match_monthly = re.search(r'(\d{4})(\d{2})', filename)
            
            if date_match_daily:
                year = int(date_match_daily.group(1))
                month = int(date_match_daily.group(2))
                day = int(date_match_daily.group(3))
            elif date_match_monthly:
                year = int(date_match_monthly.group(1))
                month = int(date_match_monthly.group(2))

            items.append({
                "main_category": main_category,
                "study_type": study_type,
                "zone_type": zone_type,
                "year": year,
                "month": month,
                "day": day,
                "publication_date": pub_date,
                "filename": filename,
                "source_url": link
            })
            
        df_catalog = pd.DataFrame(items)
        #print(df_catalog)
        try:
            
            con = get_db_connection()
            con.execute("BEGIN TRANSACTION;")
            con.sql("CREATE TABLE IF NOT EXISTS bronze.catalog AS SELECT * FROM df_catalog LIMIT 0;")
            
            # Upsert logic
            con.sql("""--sql
                MERGE INTO bronze.catalog AS target
                USING df_catalog AS source
                ON target.filename = source.filename 
                AND target.publication_date = source.publication_date
                WHEN NOT MATCHED THEN
                    INSERT BY NAME;
            """)
            con.sql(f"SELECT count(*)as inserted_rows FROM bronze.catalog").show()

            con.execute("COMMIT;")
            print("Catalogo actualizado.")
        except Exception as e:
            con.execute("ROLLBACK;")
            print(f"Error detectado: {e}. ROLLBACK")
            raise e
        finally:
            con.close()
            
    @task()
    def get_trips_urls(year: int, zones: list, month=None):
        con= get_db_connection()
        print(f"Procesando Viajes: Año {year}, Mes {month}, Zona {zones}")
        urls_to_process = []
        try :
            for zone in zones:
                urls_query = f"""
                    SELECT source_url 
                    FROM bronze.catalog 
                    WHERE year = {year} 
                    -- AND month = {month}
                    AND zone_type = '{zone}' 
                    AND main_category = 'Estudios Basicos'
                    AND study_type = 'Viajes' 
                    AND filename LIKE '%.csv.gz'
                """
                if month is None: pass # no month chosen load whole year

                elif isinstance(month, int):
                    urls_query += f" AND month = {month}"
                    
                elif isinstance(month, (list, tuple, range)):
                    months_str = ",".join(map(str, month))
                    urls_query += f" AND month IN ({months_str})"

                #files_df = con.sql(urls_query).df()

                results = con.sql(urls_query).fetchall()
                subset_results = results[:]

                for row in subset_results:
                    url = row[0]
                    # Creamos un diccionario para pasar a la siguiente tarea
                    urls_to_process.append({
                        "url": url,
                        "zone": zone
                    })
        finally:
            con.close()
        print(f"Total de archivos encontrados para procesar: {len(urls_to_process)}")
        return urls_to_process
    
    @task()
    def job_batch_params(file_info: list):
        batch_configs = []

        for file in file_info:
            url = file['url']
            zone = file['zone']

            sql_logic = f"""
                INSTALL httpfs; LOAD httpfs;
                BEGIN TRANSACTION;
                
                
                CREATE TABLE IF NOT EXISTS bronze.trips AS 
                SELECT *, '{zone}' as zone_type, current_timestamp as ingestion_date, 
                       try_strptime(fecha::VARCHAR || LPAD(periodo::VARCHAR, 2, '0'), '%Y%m%d%H') as date
                FROM read_csv('{url}', header=True, filename=True, union_by_name=True, ignore_errors=True, all_varchar=True) 
                LIMIT 0;

                
                DELETE FROM bronze.trips WHERE filename = '{url}';

                
                INSERT INTO bronze.trips BY NAME
                SELECT *, '{zone}' as zone_type, current_timestamp as ingestion_date,
                       try_strptime(fecha::VARCHAR || LPAD(periodo::VARCHAR, 2, '0'), '%Y%m%d%H') as date
                FROM read_csv('{url}', header=True, filename=True, union_by_name=True, null_padding=True, ignore_errors=True, all_varchar=True);
                
                COMMIT;
            """.replace('\n', ' ').strip()

            batch_configs.append({
                "environment": [
                    {"name": "SQL_QUERY", "value": sql_logic},
                    {"name": "memory", "value": "6GB"},
                    {"name": "AWS_DEFAULT_REGION", "value": "eu-central-1"},
                    {"name": "USUARIO_POSTGRES", "value": "neondb_owner"},
                    {"name": "CONTR_POSTGRES", "value": pg.password},
                    {"name": "HOST_POSTGRES", "value": pg.host},
                    {"name": "RUTA_S3_DUCKLAKE", "value": "s3://yena-s3-ducklake"}
                ]
            })
            
        return batch_configs


    @task()
    def ingest_trips(file_info: dict):
        """Ingesta de viajes basada en lo que hay en el catálogo"""
        url = file_info['url']
        zone = file_info['zone']

        con = get_db_connection()
        try: 
            source_query = f"""
                SELECT *, '{zone}' as zone_type, current_timestamp as ingestion_date, try_strptime(fecha::VARCHAR || LPAD(periodo::VARCHAR, 2, '0'), '%Y%m%d%H') as date
                FROM read_csv('{url}', header=True, filename=True, union_by_name=True, null_padding=True, ignore_errors=True, all_varchar=True)
            """
            con.execute("BEGIN TRANSACTION;")


            con.sql(f"CREATE TABLE IF NOT EXISTS bronze.trips AS {source_query} LIMIT 0;")
            con.sql("ALTER TABLE bronze.trips SET PARTITIONED BY (zone_type, YEAR(date), MONTH(date) );")
            delete_query = "DELETE FROM bronze.trips WHERE filename = ?"

            con.execute(delete_query, [url])

            insert_query = f"""
            INSERT INTO bronze.trips BY NAME
                {source_query}
            """
            con.sql(insert_query)
            count = con.sql(f"SELECT count(*) FROM bronze.trips WHERE filename = '{url}'").fetchone()[0]
            print(f"Insertadas {count} filas para el archivo {url}")

            con.commit() # Guardar cambios
            print("Commit realizado.")
        except Exception as e:
            con.rollback()
            print(f"Error processing {url}: {e}")
            raise e
        finally:
            con.close()
        
    @task()
    def ingest_zone_geometries(zone_list: list):
        """Descarga SHP, extrae y carga a DuckDB usando directorios temporales"""
        con = get_db_connection()
        zone_dic = {"municipios":"municiples", "distritos":"districts", "gaus":"gaus"}
        
        for zone in zone_list:
            print(f"Procesando geometría para: {zone}")

            file_info = con.sql(f"""
                SELECT source_url, filename, publication_date
                FROM bronze.catalog 
                WHERE (main_category = 'Zonificacion' OR main_category = 'Otros')
                AND (filename ILIKE '%{zone}%') 
                AND (filename ILIKE '%.shp' OR filename ILIKE '%.shx' OR filename ILIKE '%.dbf' OR filename ILIKE '%.prj' OR filename ILIKE '%.csv')
            """).df()
            
            if file_info.empty:
                continue

            # Usamos un directorio temporal que se borra al terminar el bloque 'with'
            with tempfile.TemporaryDirectory() as tmp_dir:
                # Descargar archivos
                for index, row in file_info.iterrows():
                    url = row['source_url']
                    fname = url.split('/')[-1]
                    path = os.path.join(tmp_dir, fname)
                    
                    r = requests.get(url)
                    with open(path, 'wb') as f:
                        f.write(r.content)
                
                # Definir rutas locales dentro del temp dir
                shp_path = os.path.join(tmp_dir, f"zonificacion_{zone}.shp")
                shp_centroid_path = os.path.join(tmp_dir, f"zonificacion_{zone}_centroides.shp")
                csv_path = os.path.join(tmp_dir, f"nombres_{zone}.csv")
                
                # Verificar existencia
                if not (os.path.exists(shp_path) and os.path.exists(csv_path)):
                    print(f"Faltan archivos SHP/CSV para {zone}")
                    continue

                main_source_url = file_info.iloc[0]['source_url']
                
                target_table = f"bronze.{zone_dic.get(zone, zone)}_info"
                
                # Query usando st_read sobre el path temporal
                source_query = f"""
                    SELECT 
                        CAST(t1.ID AS VARCHAR) as id_{zone_dic.get(zone)},
                        t2.name as name_{zone_dic.get(zone)},
                        '{zone_dic.get(zone)}' as zone_type,
                        'mitma' as source,
                        '{main_source_url}' as source_url,      
                        current_timestamp as ingestion_date,
                        t1.geom as geometry,
                        t3.geom as centroid
                    FROM st_read('{shp_path}') t1
                    LEFT JOIN read_csv('{csv_path}', delim='|', header=True, auto_detect=True) t2 
                    ON CAST(t1.ID AS VARCHAR) = CAST(t2.ID AS VARCHAR)
                LEFT JOIN st_read('{shp_centroid_path}') t3 ON CAST(t1.ID AS VARCHAR) = CAST(t3.ID AS VARCHAR)
                """
                try:
                    con.begin()
                    #con.sql(f"DROP TABLE {target_table}")
                    con.sql(f"CREATE TABLE IF NOT EXISTS {target_table} AS {source_query} LIMIT 0")

                    # Insertar solo nuevos (lógica simplificada)
                    con.sql(f"""
                        MERGE INTO {target_table} AS target
                        USING ({source_query}) AS sc
                        ON target.source_url = sc.source_url
                        AND target.id_{zone_dic.get(zone)} = sc.id_{zone_dic.get(zone)}
                        WHEN NOT MATCHED THEN
                        INSERT BY NAME;
                    
                    """)
                    con.commit()
                    con.sql(f"SELECT count(*) as inserted_rows FROM {target_table};").show()
                except Exception as e:
                    con.rollback()
                    print(f"Error processing {url}: {e}")
                    raise e
        con.close()



    @task()
    def ingest_relations():
        """Carga relaciones INE-MITMA"""
        con = get_db_connection()
        file_info = con.sql(f"""
            SELECT source_url, filename, publication_date
            FROM bronze.catalog 
            WHERE (main_category = 'Zonificacion' OR main_category = 'Otros')
                AND (filename ILIKE '%relacion_ine%') 

                """).df()
        if file_info.empty : 
            print("'relacion' data not found in bronze.catalog")
            pass
        publication_date = file_info["publication_date"].iloc[0]
        #print(publication_date)
        urls = file_info["source_url"].tolist()
        urls_sql_list = str(urls).replace('[', '').replace(']', '')
        print(urls_sql_list)
        source_query = f"""--sql
                    SELECT 
                        *,
                        filename as source_url,
                        current_timestamp as ingestion_date, --añadir columna para controlar fecha de ingesta
                        '{publication_date}' as publication_date
                        FROM read_csv(
                                [{urls_sql_list}],
                                header=True, 
                                
                                union_by_name=True,
                                null_padding=True,
                                ignore_errors=True,
                                all_varchar=True)-- Leemos todo como texto para evitar fallos de tipo antes de castear
                                """
        try:
            con.begin()

            con.sql(f"""
                CREATE TABLE IF NOT EXISTS  bronze.ine_mitma_zones AS
                {source_query} LIMIT 0;
                """)

            con.sql(f"""
                    MERGE INTO bronze.ine_mitma_zones as target

                        USING ({source_query})  AS new_data
                        ON target.seccion_ine = new_data.seccion_ine
                        AND target.distrito_ine = new_data.distrito_ine
                        AND target.municipio_ine = new_data.municipio_ine
                        AND target.distrito_mitma = new_data.distrito_mitma
                        AND target.municipio_mitma = new_data.municipio_mitma
                        AND target.gau_mitma = new_data.gau_mitma
                        WHEN NOT MATCHED THEN
                            INSERT BY NAME;
                        """)
            con.sql(f"SELECT count(*) as inserted_rows FROM bronze.ine_mitma_zones").show()
            con.commit()
        except Exception as e:
                    con.rollback()
                    print(f"Error processing {urls}: {e}")
                    raise e
        finally:
            con.close()

    @task()
    def parse_rent_ine():

        TABLE_ID = "30824"  # 30824 total albacete : 30656
        URL = f"https://servicios.ine.es/wstempus/js/es/DATOS_TABLA/{TABLE_ID}?tip=AM"


        # request json file from ine 
        try:
            # request json file of the table 
            response = requests.get(URL)
            response.raise_for_status()
            resultados = []
            #print(response.text)
            # print(type(response.json()))
            for entrada in response.json():
                # Filtro: Solo Distritos
                #print(entrada)
                es_distrito = False
                codigo_distrito = "N/A"
                nombre_distrito = "N/A"
                
                #Test metadata is not empty
                metadata = entrada.get("MetaData", [])
                if metadata is None: continue
                for meta in entrada.get("MetaData", []):
                    if meta.get("T3_Variable") == "Distritos":
                        es_distrito = True
                        codigo_distrito = meta.get("Codigo")
                        nombre_distrito = meta.get("Nombre")
                        break
                
                if not es_distrito:
                    continue

                # Limpieza del nombre del concepto
                nombre_completo = entrada.get("Nombre", "")
                try:
                    concepto = nombre_completo.split('.')[-2].strip()
                except:
                    concepto = nombre_completo
                if concepto != "Renta neta media por persona":
                    continue
                else:
                    concepto = "renta_media"
                #print(concepto)
                # Filtro de Años
                for dato in entrada.get("Data", []):
                    anyo = dato.get("Anyo")
                    if 2021 <= anyo <=2024:
                        resultados.append({
                            "ine_district": codigo_distrito,
                            "name": nombre_distrito,
                            "concept": concepto,
                            "year": f"{anyo}_value",
                            "avg_net_income": dato.get("Valor"),
                            "source_url": URL
                        })

            if resultados:
                df = pd.DataFrame(resultados)

                df_pivot = df.pivot_table(
                            index=['ine_district', 'name', 'concept',"source_url"],
                            columns='year',
                            values='avg_net_income'

                        ).reset_index()
                print(f" Parsed {len(df_pivot)} rows from INE {concepto}" )

            else:
                print("Rent data not found.")
        except Exception as e:
            print(f"Error: {e}")


        if df_pivot.empty : 
            print("INE data not found")
            pass
        table_name = df_pivot["concept"][0]
        #print(f"bronze.{table_name}")
        
        try:
            con = get_db_connection()
            con.begin()
            con.sql(f"CREATE TABLE IF NOT EXISTS bronze.{table_name} AS SELECT *,current_timestamp as ingestion_date FROM df_pivot LIMIT 0;")
            for colname in df_pivot.columns.tolist():
                    if colname.startswith("ine_"):
                        ine_id = colname
                    con.sql(f"""ALTER TABLE bronze.{table_name} ADD COLUMN IF NOT EXISTS "{colname}" VARCHAR;""") 

                # loop para meter todas las columnas del dataframe, en caso de actualizacion de datos del ine (2025) se añadiran estos datos sin error

            #con.sql(f"DESCRIBE TABLE bronze.{table_name}").show()
            con.sql(f"""
                    MERGE INTO bronze.{table_name} AS target
                    USING (SELECT *, 
                                current_timestamp as ingestion_date
                                FROM df_pivot) AS sc
                    ON target.{ine_id} = sc.{ine_id}
                    AND target.concept = sc.concept
                    AND target.name = sc.name
                    WHEN NOT MATCHED THEN
                        INSERT BY NAME;

                """)
            con.commit()    
        except Exception as e:
                    con.rollback()
                    print(f"Error processing {table_name}: {e}")
                    raise e
        finally:
            con.close()

    @task()
    def parse_population_ine():

        # TABLA DE POBLACION

        TABLE_ID = "66595"  # 66595 total, albacete : 69095
        URL = f"https://servicios.ine.es/wstempus/js/es/DATOS_TABLA/{TABLE_ID}?tip=AM"

        print (f"Parsing population data from {URL}")
        # 2. Hacemos la petición
        try:
            
            # request json file of the table 
            response = requests.get(URL)
            response.raise_for_status()
            resultados = []
            # print(response.text)
            # print(type(response.json()))
            for entrada in response.json():
                metadata = entrada.get("MetaData", [])
                
                # --- Variables de control (Flags) ---
                es_seccion = False
                codigo_seccion = None
                nombre_seccion = None
                
                es_total_actividad = False
                es_total_pais = False
                es_total_sexo = False
                
                # --- Iterar metadatos para verificar filtros ---
                for meta in metadata:
                    variable = meta.get("T3_Variable")
                    nombre = meta.get("Nombre") # Ejemplo: "Total", "Ocupado/a", "Extranjero"
                    
                    # 1. Filtro Geográfico: Solo Secciones
                    if variable == "Secciones":
                        es_seccion = True
                        codigo_seccion = meta.get("Codigo") # Ejemplo: 0100101001
                        nombre_seccion = meta.get("Nombre")

                    # 2. Filtro Actividad: Debe ser "Total" (para excluir "Ocupados", "Estudiantes")
                    elif variable == "Relación con la actividad" and nombre == "Total":
                        es_total_actividad = True
                        
                    # 3. Filtro Nacionalidad: Debe ser "Total" (para excluir "Española", "Extranjera")
                    elif variable in ["Países", "Nacionalidad"] and nombre == "Total":
                        es_total_pais = True

                    # 4. Filtro Sexo: Debe ser "Total"
                    elif variable == "Sexo" and nombre == "Total":
                        es_total_sexo = True

                # --- Decisión de Guardado ---
                # Solo procesamos si es una Sección Y además es el TOTAL de todas las categorías
                if es_seccion and es_total_actividad and es_total_pais and es_total_sexo:
                    
                    for dato in entrada.get("Data", []):

                        anyo = dato.get("Anyo")
                        if 2021 <= anyo <= 2024: 
                            resultados.append({
                                "ine_section": codigo_seccion,
                                "name": nombre_seccion,
                                "concept": "poblacion_total", 
                                "year": f"{anyo}_value",
                                "total_population": dato.get("Valor"),
                                "source_url": URL
                            })


            if resultados:
                df = pd.DataFrame(resultados)
                
                df_pivot = df.pivot_table(
                    index=['ine_section', 'name', 'concept', 'source_url'],
                    columns='year',
                    values='total_population'
                ).reset_index()
                print(f" Parsed {len(df_pivot)} rows from INE Poblacion Total" )
                df_pivot.columns.name = None
                
            else:
                print("Population data not found.")
        except Exception as e:
            print(f"Error: {e}")
        
        if df_pivot.empty : 
            print("INE data not found")
            pass
        table_name = df_pivot["concept"][0]
        #print(f"bronze.{table_name}")

        try:
            con = get_db_connection()
            con.begin()
            con.sql(f"CREATE TABLE IF NOT EXISTS bronze.{table_name} AS SELECT *,current_timestamp as ingestion_date FROM df_pivot LIMIT 0;")
            for colname in df_pivot.columns.tolist():
                    if colname.startswith("ine_"):
                        ine_id = colname
                    con.sql(f"""ALTER TABLE bronze.{table_name} ADD COLUMN IF NOT EXISTS "{colname}" VARCHAR;""") 

                # loop para meter todas las columnas del dataframe, en caso de actualizacion de datos del ine (2025) se añadiran estos datos sin error

            # con.sql(f"DESCRIBE TABLE bronze.{table_name}").show()
            con.sql(f"""
                    MERGE INTO bronze.{table_name} AS target
                    USING (SELECT *, 
                        current_timestamp as ingestion_date
                        FROM df_pivot) AS sc
                    ON target.ine_section = sc.ine_section
                    AND target.concept = sc.concept
                    AND sc.name = sc.name
                    WHEN NOT MATCHED THEN
                        INSERT BY NAME;

                    
                """)
            con.commit()
        except Exception as e:
                    con.rollback()
                    print(f"Error processing {table_name}: {e}")
                    raise e
        finally:
            con.close()

    

    # --- DEFINICIÓN DEL FLUJO DEL DAG ---
    

    task_init = init_schema()
    task_catalog = ingest_catalog()

    # Viajes (Trips)
    task_urls = get_trips_urls(year=2023, month=[3,4,5,6,7,8,9,10,11,12], zones=["Distritos","Municipios", "GAU"])
    #task_trips = ingest_trips.expand(file_info=task_urls)
    batch_overrides = job_batch_params(task_urls)

    ingest_tasks = BatchOperator.partial(
        task_id='ingesta_batch_dinamica',
        job_name='ingesta-trips-worker',
        job_queue='duck_jobque',
        job_definition='duck_jobdef',
        region_name='eu-central-1',
        # Opcional: Aumentar timeout porque son cargas pesadas
        
    ).expand(container_overrides=batch_overrides)

    # Geometrías
    task_zones = ingest_zone_geometries(zone_list=["distritos", "municipios", "gaus"])
    
    # Relaciones
    task_rel = ingest_relations()
    
    # INE (Independiente del catálogo de MITMA, pero depende de init)
    task_ine_rent = parse_rent_ine()
    task_ine_population = parse_population_ine()

    # Orquestación (Dependencias)
    task_init >> task_catalog
    
    task_catalog >> task_urls
    #task_urls >> task_trips
    task_catalog >> task_zones
    task_catalog >> task_rel
    
    task_init >> task_ine_rent
    task_init >> task_ine_population

# Instanciamos el DAG
mobility_ingestion = mobility_dag()