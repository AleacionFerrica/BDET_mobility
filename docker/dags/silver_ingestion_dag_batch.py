from airflow.sdk import Asset, dag, task
from airflow.sdk.bases.hook import BaseHook
from airflow.providers.amazon.aws.operators.batch import BatchOperator
import duckdb
import pandas as pd
import requests
import json
from datetime import datetime, timedelta
import os
from pathlib import Path
import xml.etree.ElementTree as ET
import calendar
import re
pg = BaseHook.get_connection("neon_postgres")
aws = BaseHook.get_connection("aws_default")
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/usr/local/airflow")
DB_PATH = "include/mobility.ducklake"

# Función auxiliar para conectar a DuckDB con las extensiones necesarias
def get_db_connection():
    con = duckdb.connect( )
    con.sql("INSTALL ducklake; LOAD ducklake;")
    con.sql("INSTALL spatial; LOAD spatial;")
    con.sql(f"ATTACH '{DB_PATH}' AS my_ducklake;")
    con.sql("USE my_ducklake;")
    
    return con
def get_db_connection():

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
    dag_id='ingesta_silver_batch',
    #schedule_interval='@weekly', # Ejecutar semanalmente o cuando quieras
    #start_date=days_ago(1),
    default_args={'owner': 'airflow','retries': 3,'retry_delay': timedelta(minutes=1)},

    
    catchup=False,
    max_active_tasks=64,
    tags=['master', 'duckdb', 'mitma', 'silver'] )

def silver_mobility_dag():

    @task()
    def init_schema():
        con = get_db_connection()
        con.sql("CREATE SCHEMA IF NOT EXISTS silver;")
        con.close()
        print(f"Silver schema created")

    @task()
    def master_zones():
        print("Searching new zone codes...")
        con = get_db_connection()
        con.sql("""--sql
            CREATE TABLE IF NOT EXISTS silver.dim_zones (
                id_zone INTEGER,     -- numero incremental identificador de la zona
                original_id VARCHAR,
                zone_type VARCHAR,
                source VARCHAR,
                ingestion_date TIMESTAMP
            );
        """)
            
        print("Master Table 'silver.dim_zones' .")

        # Buscamos todos los códigos que existen en Bronze
        query_candidatos = """--sql
            WITH gaus_info AS (SELECT * FROM bronze.gaus_info 
                        WHERE id_gaus != 'NA'
                        AND id_gaus IS NOT NULL
                                AND id_gaus NOT LIKE 'FR%' 
                                AND id_gaus NOT LIKE 'PT%' 
                                AND id_gaus NOT LIKE 'IT%' 
                                AND id_gaus NOT LIKE 'DE%' 
                                AND id_gaus NOT LIKE 'UK%' 
                                AND id_gaus NOT LIKE 'US%'
                                AND id_gaus NOT LIKE 'EXT%'
                        
                        ),

                districts_info AS (SELECT * FROM bronze.districts_info  
                        WHERE id_districts != 'NA' 
                        AND id_districts IS NOT NULL
                            AND id_districts NOT LIKE 'FR%' 
                            AND id_districts NOT LIKE 'PT%' 
                            AND id_districts NOT LIKE 'IT%' 
                            AND id_districts NOT LIKE 'DE%' 
                            AND id_districts NOT LIKE 'UK%' 
                            AND id_districts NOT LIKE 'US%'
                            AND id_districts NOT LIKE 'EXT%'
                        ),

                municipless_info AS (SELECT * FROM bronze.municiples_info 
                        WHERE id_municiples != 'NA' 
                        AND id_municiples IS NOT NULL
                            AND id_municiples NOT LIKE 'FR%' 
                            AND id_municiples NOT LIKE 'PT%' 
                            AND id_municiples NOT LIKE 'IT%' 
                            AND id_municiples NOT LIKE 'DE%' 
                            AND id_municiples NOT LIKE 'UK%' 
                            AND id_municiples NOT LIKE 'US%'
                            AND id_municiples NOT LIKE 'EXT%'
                        
                        ),

                ine_mitma_zones AS (SELECT * FROM bronze.ine_mitma_zones 
                        WHERE seccion_ine != 'NA' 
                        AND seccion_ine IS NOT NULL 
                        AND distrito_ine != 'NA' 
                        AND municipio_ine != 'NA'),

            all_codes AS (
                SELECT TRIM(id_gaus) as original_id, 'gaus' as zone_type, 'mitma' as source FROM gaus_info
                UNION ALL
                SELECT TRIM(id_districts) as original_id, 'districts' as zone_type, 'mitma' as source FROM districts_info
                UNION ALL
                SELECT TRIM(id_municiples) as original_id, 'municiples' as zone_type, 'mitma' as source FROM municipless_info
                UNION ALL
                SELECT  TRIM(gau_mitma) as original_id, 'gaus' as zone_type , 'mitma' as source FROM ine_mitma_zones
                UNION ALL
                SELECT  TRIM(distrito_mitma) as original_id, 'districts' as zone_type, 'mitma' as source FROM ine_mitma_zones
                UNION ALL
                SELECT TRIM(municipio_mitma) as original_id, 'municiples'  as zone_type, 'mitma' as source FROM ine_mitma_zones

                UNION ALL
                SELECT TRIM(distrito_ine) as original_id, 'districts' as zone_type, 'ine' as source FROM ine_mitma_zones
                UNION ALL
                SELECT TRIM(municipio_ine) as original_id, 'municiples' as zone_type, 'ine' as source FROM ine_mitma_zones
                UNION ALL
                SELECT TRIM(seccion_ine) as original_id, 'sections' as zone_type, 'ine' as source FROM ine_mitma_zones

                UNION ALL
                SELECT TRIM(ine_district) as original_id, 'districts' as zone_type, 'ine' as source FROM bronze.renta_media
                UNION ALL
                SELECT TRIM(ine_section) as original_id, 'sections' as zone_type, 'ine' as source FROM bronze.poblacion_total

            ),
            distinct_candidates AS (
            SELECT DISTINCT original_id,  zone_type, source FROM all_codes WHERE original_id IS NOT NULL
        )
        SELECT * FROM distinct_candidates
            """
        
        #con.sql("DROP TABLE IF EXISTS silver.dim_zones CASCADE")
        # INSERT 
        #  a) Calculamos el MAX(id_zone) actual (si es NULL, usamos 0)
        #  b) A los nuevos les sumamos ROW_NUMBER() + ese MAX
        #con.sql(query_candidatos).show()

        query_new_codes =f"""--sql
            --INSERT INTO silver.dim_zones (id_zone, original_id, zone_type, source, ingestion_date)
                WITH current_state AS (
                -- Obtenemos el último ID usado. Si la tabla está vacía, devuelve 0.
                SELECT COALESCE(MAX(id_zone), 0) as max_current_id 
                FROM silver.dim_zones
            ),
            new_codes_to_insert AS (
                -- Seleccionamos solo los códigos que NO existen en la tabla maestra
                SELECT 
                    c.original_id, 
                    c.zone_type,
                    c.source
                FROM ({query_candidatos}) c
                LEFT JOIN silver.dim_zones existing 
                    ON c.original_id = existing.original_id
                WHERE existing.original_id IS NULL
            )
            SELECT 
                -- FÓRMULA MAESTRA:
                (SELECT max_current_id FROM current_state) + ROW_NUMBER() OVER(ORDER BY nc.original_id) as id_zone,
                
                nc.original_id as original_id,
                nc.zone_type as zone_type,
                nc.source as source,
                current_timestamp as ingestion_date
            FROM new_codes_to_insert nc

        """

        #con.sql(query_new_codes)
        try : 
            con = get_db_connection()
            con.sql(f"CREATE  TABLE IF NOT EXISTS silver.dim_zones AS {query_new_codes} LIMIT 0;")

            con.sql(f"""--sql
                MERGE INTO silver.dim_zones as target
                    USING (SELECT * FROM ({query_new_codes})) as sc
                    ON target.original_id = sc.original_id
                    AND target.zone_type = sc.zone_type
                    AND target.source = sc.source
                    WHEN NOT MATCHED THEN
                        INSERT BY NAME; 

            """)
            

            
            con.commit()
            con.sql("SELECT count(*) inserted_rows FROM silver.dim_zones").show()
        except Exception as e:
            con.execute("ROLLBACK;")
            print(f"Error detectado: {e}. ROLLBACK")
            raise e
        finally:
            con.close()

    @task
    def master_zones_batch():

        sql_logic = f"""
        BEGIN TRANSACTION;

        CREATE TABLE IF NOT EXISTS  silver.dim_zones (
            id_zone INTEGER,
            original_id VARCHAR,
            zone_type VARCHAR,
            source VARCHAR,
            ingestion_date TIMESTAMP
        );

        WITH gaus_info AS (SELECT * FROM  bronze.gaus_info WHERE id_gaus != 'NA' AND id_gaus IS NOT NULL),
             districts_info AS (SELECT * FROM  bronze.districts_info WHERE id_districts != 'NA' AND id_districts IS NOT NULL),
             municipless_info AS (SELECT * FROM  bronze.municiples_info WHERE id_municiples != 'NA' AND id_municiples IS NOT NULL),
             ine_mitma_zones AS (SELECT * FROM  bronze.ine_mitma_zones WHERE seccion_ine != 'NA' AND seccion_ine IS NOT NULL AND distrito_ine != 'NA' AND municipio_ine != 'NA'),

             all_codes AS (
                SELECT TRIM(id_gaus) as original_id, 'gaus' as zone_type, 'mitma' as source FROM gaus_info
                UNION ALL
                SELECT TRIM(id_districts) as original_id, 'districts' as zone_type, 'mitma' as source FROM districts_info
                UNION ALL
                SELECT TRIM(id_municiples) as original_id, 'municiples' as zone_type, 'mitma' as source FROM municipless_info
                UNION ALL
                SELECT TRIM(gau_mitma) as original_id, 'gaus' as zone_type , 'mitma' as source FROM ine_mitma_zones
                UNION ALL
                SELECT TRIM(distrito_mitma) as original_id, 'districts' as zone_type, 'mitma' as source FROM ine_mitma_zones
                UNION ALL
                SELECT TRIM(municipio_mitma) as original_id, 'municiples'  as zone_type, 'mitma' as source FROM ine_mitma_zones
                UNION ALL
                SELECT TRIM(distrito_ine) as original_id, 'districts' as zone_type, 'ine' as source FROM ine_mitma_zones
                UNION ALL
                SELECT TRIM(municipio_ine) as original_id, 'municiples' as zone_type, 'ine' as source FROM ine_mitma_zones
                UNION ALL
                SELECT TRIM(seccion_ine) as original_id, 'sections' as zone_type, 'ine' as source FROM ine_mitma_zones
                UNION ALL
                SELECT TRIM(ine_district) as original_id, 'districts' as zone_type, 'ine' as source FROM  bronze.renta_media
                UNION ALL
                SELECT TRIM(ine_section) as original_id, 'sections' as zone_type, 'ine' as source FROM  bronze.poblacion_total
            ),
            
            distinct_candidates AS (
                SELECT DISTINCT original_id, zone_type, source FROM all_codes WHERE original_id IS NOT NULL
            ),

            current_state AS (
                SELECT COALESCE(MAX(id_zone), 0) as max_current_id FROM  silver.dim_zones
            ),
            
            new_codes_to_insert AS (
                SELECT c.original_id, c.zone_type, c.source
                FROM distinct_candidates c
                LEFT JOIN  silver.dim_zones existing 
                    ON c.original_id = existing.original_id 
                    AND c.zone_type = existing.zone_type -- Importante join también por tipo
                WHERE existing.original_id IS NULL
            ),
            
            final_selection AS (
                SELECT 
                    (SELECT max_current_id FROM current_state) + ROW_NUMBER() OVER(ORDER BY nc.original_id) as id_zone,
                    nc.original_id,
                    nc.zone_type,
                    nc.source,
                    current_timestamp as ingestion_date
                FROM new_codes_to_insert nc
            )

        MERGE INTO  silver.dim_zones as target
        USING (SELECT * FROM final_selection) as sc
        ON target.original_id = sc.original_id
                AND target.zone_type = sc.zone_type
                AND target.source = sc.source
                WHEN NOT MATCHED THEN
                    INSERT BY NAME; 

        COMMIT;
    """.replace('\n', ' ').strip()
        
        batch = [
                                {"environment": [
                                    {"name": "SQL_QUERY", "value": sql_logic},
                                    {"name": "memory", "value": "15GB"},
                                    {"name": "AWS_DEFAULT_REGION", "value": "eu-central-1"},
                                    {"name": "USUARIO_POSTGRES", "value": "neondb_owner"},
                                    {"name": "CONTR_POSTGRES", "value": pg.password},
                                    {"name": "HOST_POSTGRES", "value": pg.host},
                                    {"name": "RUTA_S3_DUCKLAKE", "value": "s3://yena-s3-ducklake"}
                                ]
                            }]
        return batch

    @task()
    def create_silver_trips():
        con = get_db_connection()
        try: 
            con.begin()
            #con.sql("DROP TABLE silver.od_trips")
            con.sql("""--sql
                CREATE TABLE IF NOT EXISTS  silver.od_trips (
                    date TIMESTAMP,
                    --hour_period INTEGER,
                    zone_type VARCHAR,
                    -- od
                    id_origin VARCHAR,
                    id_destination VARCHAR,
                    --origin_activity VARCHAR,
                    --destination_activity VARCHAR,
                    
                    distance_group_km VARCHAR,

                    --distance
                    n_trips DOUBLE,
                    trips_total_length_km DOUBLE,

                    origin_activity_std boolean,
                    destination_activity_std boolean,

                    ingestion_date TIMESTAMP
                );
            """)
            con.sql("ALTER TABLE silver.od_trips SET PARTITIONED BY (zone_type, YEAR(date), MONTH(date) );")
            con.commit()
        except Exception as e:
            con.execute("ROLLBACK;")
            print(f"Error detectado: {e}. ROLLBACK")
            raise e
        finally:
            con.close()

    @task()
    def sql_batch(zones: list, years: list, months: list, days: list):
        batch_configs = []
        
        zone_dic = {"Municipios": "municiples", "Distritos": "districts", "GAU": "gaus"}
        
        for zone in zones:
            for year in years:
                for month in months:
                    _, last_day_of_month = calendar.monthrange(year, month)
                    for day in days:
                        if day > last_day_of_month:

                            continue
                        target_zone = zone_dic.get(zone, zone.lower())
                        
                        # --- CONSTRUCCIÓN DEL SQL ---
                        # Nota: Usamos una sola cadena con transacciones para atomicidad
                        date_str = f"{year}-{month:02d}-{day:02d}"
                        sql_logic = f"""
                            BEGIN TRANSACTION;

                            DELETE FROM silver.od_trips
                            WHERE zone_type = '{target_zone}'
                            AND date >= '{date_str} 00:00:00' 
                            AND date < '{date_str} 23:59:59.999';

                            INSERT INTO silver.od_trips BY NAME
                            WITH dim_zn AS MATERIALIZED (
                                SELECT
                                    original_id,
                                    id_zone

                                FROM silver.dim_zones 
                                WHERE source = 'mitma'
                                    AND zone_type = '{target_zone}')
                            SELECT 
                                br.date,

                                CASE WHEN br.zone_type = 'Distritos' THEN 'districts' 
                                    WHEN br.zone_type = 'Municipios' THEN 'municiples' 
                                    WHEN br.zone_type = 'GAU' THEN 'gaus' END as zone_type,

                                d_o.id_zone AS id_origin,
                                d_d.id_zone AS id_destination,

                                TRIM(TRY_CAST(distancia AS VARCHAR)) as distance_group_km,

                                ROUND(SUM(TRY_CAST(viajes AS DOUBLE)), 1) as n_trips, 
                                ROUND(SUM(TRY_CAST(viajes_km AS DOUBLE)), 2) as trips_total_length_km,

                                CURRENT_TIMESTAMP AS ingestion_date

                            FROM bronze.trips br 
                            LEFT JOIN dim_zn d_o
                                ON br.origen = d_o.original_id 
                            LEFT JOIN dim_zn d_d 
                                ON br.destino = d_d.original_id

                            WHERE TRIM(br.zone_type) = '{zone}'
                            AND date >= '{date_str} 00:00:00' 
                            AND date < '{date_str} 23:59:59'

                            GROUP BY 1, 2, 3, 4, 5;

                            COMMIT;
                        """.replace('\n', ' ').strip() 
                        
                        # Añadimos la configuración a la lista
                        batch_configs.append({"resourceRequirements": [
                            {"type": "VCPU", "value": "1"},
                            {"type": "MEMORY", "value": "4096"} 
                             ],
                                "environment": [
                                    {"name": "SQL_QUERY", "value": sql_logic},
                                    {"name": "memory", "value": "4GB"},
                                    {"name": "AWS_DEFAULT_REGION", "value": "eu-central-1"},
                                    {"name": "USUARIO_POSTGRES", "value": "neondb_owner"},
                                    {"name": "CONTR_POSTGRES", "value": pg.password},
                                    {"name": "HOST_POSTGRES", "value": pg.host},
                                    {"name": "RUTA_S3_DUCKLAKE", "value": "s3://yena-s3-ducklake"}
                                ]
                            })             
        print("Number of Jobs to send: ",len(batch_configs))
        return batch_configs

    @task()
    def load_silver_trips( zone_type, year, month, day):
        
        zone_dic = {"Municipios":"municiples","Distritos":"districts","GAU":"gaus"}

        
        
        print(f"Loading Silver for {zone_type} {year}/{month}/{day}")
        query = f"""--sql
        --INSERT INTO silver.od_trips
        SELECT 
            -- Dimensiones (Agrupadores)
            br.date as date,

            CASE WHEN br.zone_type = 'Distritos' THEN 'districts' 
                WHEN br.zone_type = 'Municipios' THEN 'municiples' 
                WHEN br.zone_type ='GAU' THEN 'gaus' END as zone_type,

            d_o.id_zone AS id_origin,
            d_d.id_zone AS id_destination,
            TRIM(TRY_CAST(distancia AS VARCHAR)) as distance_group_km,
        
            ROUND(SUM(TRY_CAST(viajes AS DOUBLE)), 1) as n_trips, 
            ROUND(SUM(TRY_CAST(viajes_km AS DOUBLE)), 2) as trips_total_length_km,

            CURRENT_TIMESTAMP AS ingestion_date

        FROM bronze.trips br 
        LEFT JOIN silver.dim_zones d_o
            ON br.origen = d_o.original_id AND d_o.source = 'mitma' AND d_o.zone_type = '{zone_dic[zone_type]}'
        LEFT JOIN silver.dim_zones d_d 
            ON br.destino = d_d.original_id AND d_d.source = 'mitma' AND d_d.zone_type = '{zone_dic[zone_type]}'
        WHERE 
            YEAR(date) = {year}
            AND TRIM(br.zone_type) = '{zone_type}'
            AND MONTH(date) = {month}
            AND DAY(date) = {day}
            
        """

        query += f" GROUP BY  1, 2, 3, 4, 5"

            
        #print(query)
        con = get_db_connection()
        #con.sql(query).show()
        try:

            con.commit()
        except Exception as e:
            print(e)
        try:
            

            con.begin()
            # Borramos datos previos de esa partición para evitar duplicados
            del_q = f"""
                DELETE FROM silver.od_trips
                WHERE zone_type = '{zone_dic[zone_type]}'
                AND YEAR(date) = {year}
                AND MONTH(date) = {month}
                AND DAY(date) = {day}
            """
            
            
            con.sql(del_q)

            con.sql(f"""INSERT INTO silver.od_trips BY NAME {query} ;""")
            check_q = f"""
            SELECT count(*) as inserted_rows_check 
            FROM silver.od_trips 
            WHERE zone_type = '{zone_dic[zone_type]}' 
            AND YEAR(date) = {year} AND MONTH(date) = {month} AND DAY(date) = {day}
            """
            con.sql(check_q).show()
            con.commit()
        except Exception as e:
            con.rollback()
            print(f"Error detectado en {zone_type}, {month}, {day}: {e}. ROLLBACK")
            raise e
        finally:
            con.close()

    @task()
    def load_silver_ine_mitma_zones():
    
        query = f""" --sql
                SELECT 
                    --  SECCIÓN (INE)
                    d_sec.id_zone as id_sections_ine,

                    --  DISTRITO (INE)
                    d_dis.id_zone  as id_districts_ine,

                    --  MUNICIPIO (INE)
                    d_mun.id_zone as id_municiples_ine,
                    
                    -- DISTRITO (MITMA)
                    d_dis_mit.id_zone as id_districts_mitma,

                    -- MUNICIPIO (MITMA)
                    d_mun_mit.id_zone as id_municiples_mitma,

                    --  GAU (MITMA)
                    d_gau.id_zone as id_gaus_mitma,

                    current_timestamp as ingestion_date

                FROM bronze.ine_mitma_zones br
                
                -- JOIN 1: Para Sección
                LEFT JOIN silver.dim_zones d_sec 
                    ON TRIM(br.seccion_ine) = d_sec.original_id 
                    AND d_sec.zone_type = 'sections' -- Opcional: si tu diccionario distingue tipos

                -- JOIN 2: Para Distrito INE
                LEFT JOIN silver.dim_zones d_dis 
                    ON TRIM(br.distrito_ine) = d_dis.original_id
                    AND d_dis.zone_type = 'districts' AND d_dis.source = 'ine'

                -- JOIN 3: Para Municipio INE
                LEFT JOIN silver.dim_zones d_mun 
                    ON TRIM(br.municipio_ine) = d_mun.original_id
                    AND d_mun.zone_type = 'municiples' AND d_mun.source = 'ine'

                -- JOIN 4: Para Distrito MITMA
                LEFT JOIN silver.dim_zones d_dis_mit 
                    ON TRIM(br.distrito_mitma) = d_dis_mit.original_id
                    AND d_dis_mit.zone_type = 'districts' AND d_dis_mit.source = 'mitma'

                -- JOIN 5: Para Municipio MITMA
                LEFT JOIN silver.dim_zones d_mun_mit 
                    ON TRIM(br.municipio_mitma) = d_mun_mit.original_id
                    AND d_mun_mit.zone_type = 'municiples' AND d_mun_mit.source = 'mitma'

                -- JOIN 6: Para GAU
                LEFT JOIN silver.dim_zones d_gau 
                    ON TRIM(br.gau_mitma) = d_gau.original_id
                    AND d_gau.zone_type = 'gaus'

                WHERE NULLIF(br.seccion_ine, 'NA') IS NOT NULL
            """
        con = get_db_connection()
        try: 
            
            con.begin()
            con.sql(f"CREATE TABLE IF NOT EXISTS silver.ine_mitma_zones AS {query} LIMIT 0 ")

            con.sql(f"""--sql
                    MERGE INTO silver.ine_mitma_zones AS target
                    USING ({query} )AS source
                    ON target.id_sections_ine = source.id_sections_ine 
                    AND target.id_districts_ine = source.id_districts_ine
                    AND target.id_municiples_ine = source.id_municiples_ine
                    AND target.id_districts_mitma = source.id_districts_mitma
                    AND target.id_municiples_mitma = source.id_municiples_mitma
                    AND target.id_gaus_mitma = source.id_gaus_mitma
                    WHEN NOT MATCHED THEN
                        INSERT BY NAME;
                """)
            
            con.sql(f"SELECT count(*)as inserted_rows FROM silver.ine_mitma_zones").show()
 
 
            con.commit()
        except Exception as e:
            con.execute("ROLLBACK;")
            print(f"Error detectado: {e}. ROLLBACK")
            raise e
        finally:
            con.close()

    @task
    def load_silver_zone():
        #zone_type = "districts"
        q = """ --sql
        WITH gaus AS (SELECT 
                    d.id_zone,
                    ifnull(TRIM(br.name_gaus), 'external') AS name_zone,
                    br.zone_type,
                    br.geometry,
                    br.centroid,
                    ST_PointOnSurface(br.geometry) AS visual_point,
                FROM bronze.gaus_info br
                LEFT JOIN silver.dim_zones d
                ON br.id_gaus = d.original_id AND br.zone_type = d.zone_type AND d.source = 'mitma'
                WHERE id_gaus != 'NA' 
                AND id_gaus IS NOT NULL
                    AND br.id_gaus NOT LIKE 'FR%' 
                    AND br.id_gaus NOT LIKE 'PT%' 
                    AND br.id_gaus NOT LIKE 'IT%' 
                    AND br.id_gaus NOT LIKE 'DE%' 
                    AND br.id_gaus NOT LIKE 'UK%' 
                    AND br.id_gaus NOT LIKE 'US%'
                    AND br.id_gaus NOT LIKE 'EXT%'
        ),
        municiples AS (SELECT 
                    d.id_zone,
                    ifnull(TRIM(br.name_municiples), 'external') AS name_zone,
                    br.zone_type,
                    br.geometry,
                    br.centroid,
                    ST_PointOnSurface(br.geometry) AS visual_point,
                FROM bronze.municiples_info br
                LEFT JOIN silver.dim_zones d
                ON br.id_municiples = d.original_id AND br.zone_type = d.zone_type AND d.source = 'mitma'
                WHERE id_municiples != 'NA' 
                AND br.id_municiples IS NOT NULL 
                    AND br.id_municiples NOT LIKE 'FR%' 
                    AND br.id_municiples NOT LIKE 'PT%' 
                    AND br.id_municiples NOT LIKE 'IT%' 
                    AND br.id_municiples NOT LIKE 'DE%' 
                    AND br.id_municiples NOT LIKE 'UK%' 
                    AND br.id_municiples NOT LIKE 'US%'
                    AND br.id_municiples NOT LIKE 'EXT%'
        ),
        districts AS (SELECT 
                    d.id_zone,
                    ifnull(TRIM(br.name_districts), 'external') AS name_zone,
                    br.zone_type,
                    br.geometry,
                    br.centroid,
                    ST_PointOnSurface(br.geometry) AS visual_point,
                FROM bronze.districts_info br
                LEFT JOIN silver.dim_zones d
                ON br.id_districts = d.original_id AND br.zone_type = d.zone_type AND d.source = 'mitma' 
                WHERE id_districts != 'NA' 
                AND id_districts IS NOT NULL
                    AND br.id_districts NOT LIKE 'FR%' 
                    AND br.id_districts NOT LIKE 'PT%' 
                    AND br.id_districts NOT LIKE 'IT%' 
                    AND br.id_districts NOT LIKE 'DE%' 
                    AND br.id_districts NOT LIKE 'UK%' 
                    AND br.id_districts NOT LIKE 'US%'
                    AND br.id_districts NOT LIKE 'EXT%'
        )

        SELECT * FROM  gaus
        UNION ALL 
        SELECT * FROM  municiples
        UNION ALL 
        SELECT * FROM  districts
        """
        con = get_db_connection()
        try:

            con.begin()
            con.sql(f"CREATE TABLE IF NOT EXISTS silver.zones_info AS (SELECT *, current_timestamp as ingestion_date FROM ({q})) LIMIT 0 ")

            con.sql(f"""--sql
                    MERGE INTO silver.zones_info AS target
                    USING (SELECT *, current_timestamp as ingestion_date FROM ({q})) AS source
                    ON target.id_zone = source.id_zone 
                    AND target.name_zone = source.name_zone
                    AND target.zone_type = source.zone_type
                    WHEN NOT MATCHED THEN
                        INSERT BY NAME;
                """)
            
            con.sql(f"SELECT count(*)as inserted_rows FROM silver.zones_info").show()
            con.commit()
        except Exception as e:
            con.execute("ROLLBACK;")
            print(f"Error detectado: {e}. ROLLBACK")
            raise e
        finally:
            con.close()

    @task()
    def load_zone_pairs(zone_type):
        
        pairs_query=f"""--sql
                        WITH base AS (
                                SELECT
                                    zone_type,
                                    id_zone,
                                    centroid
                                FROM silver.zones_info WHERE zone_type = '{zone_type}'

                                    )
                            
                            SELECT
                                a.zone_type as zone_type,
                                a.id_zone AS id_origin,
                                b.id_zone AS id_destination,
                                ROUND(ST_Distance(a.centroid, b.centroid) / 1000.0,3) AS distance_km --unidades en km

                            FROM base AS a
                            CROSS JOIN base AS b -- cross join para tener los pares 
                            WHERE a.id_zone < b.id_zone -- con < nos aseguramos de no tener distancia de A-B y B-A ni A-A   
                            """
        
        con = get_db_connection()
        try:

            con.begin()

            con.sql(f"""--sql
                CREATE TABLE IF NOT EXISTS silver.zone_pairs AS {pairs_query} LIMIT 0""")



            con.sql(f""" --sql
                    MERGE INTO silver.zone_pairs AS target
                    USING ({pairs_query}) AS source
                    ON target.zone_type = source.zone_type 
                    AND target.id_origin = source.id_origin
                    AND target.id_destination = source.id_destination
                    WHEN NOT MATCHED THEN
                        INSERT BY NAME;
                    
                    """)
            con.commit()
            con.sql(f"SELECT count(*)as inserted_rows FROM silver.zone_pairs WHERE zone_type = '{zone_type}' ").show()
        except Exception as e:
            con.execute("ROLLBACK;")
            print(f"Error detectado: {e}. ROLLBACK")
            raise e
        finally:
            con.close()
            
    @task()
    def population_check():
        con = get_db_connection()
        batch_configs = []
        table_name = "bronze.poblacion_total"
        target_table = "silver.spain_population"

        # Deteccion columnas de años
        cols_df = con.sql(f"DESCRIBE {table_name}").df()
        year_cols = [f"{c}" for c in cols_df['column_name'] if c.endswith('_value')]
        year_cols.sort()
        years_labels = [c.split('_')[0] for c in year_cols]

        # listas para SQL (con comillas para evitar errores de sintaxis)
        sql_col_list = "[" + ", ".join([f'"{c}"' for c in year_cols]) + "]"
        sql_year_label_list = "[" + ", ".join([f"'{y}'" for y in years_labels]) + "]"

        print(f"Years population data from: {years_labels}")
        for year in years_labels:
            con.sql(f"""SELECT 
                SUM("{year}_value") as total_{year}, 
                
                FROM bronze.poblacion_total """).show()
        # 2. Query Maestra: Unpivot + Imputación + Estadísticas + Detección de Atípicos
        query_transformacion = f"""
            WITH raw_unpivoted AS (
                SELECT 
                    d.id_zone as id_zone,
                    name as name_zone,
                    d.zone_type as zone_type,
                    d.source as source,
                    UNNEST({sql_year_label_list})::INT as year,
                    UNNEST({sql_col_list})::DOUBLE as population_raw
                FROM {table_name} br LEFT JOIN silver.dim_zones d ON br.ine_section = d.original_id AND d.source = 'ine' AND d.zone_type = 'sections'
            ),
            calc_imputation AS (
                SELECT 
                    *,
                    COALESCE(
                        population_raw, 
                        
                        LAST_VALUE(population_raw IGNORE NULLS) 
                            OVER(PARTITION BY id_zone ORDER BY year 
                                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
                        
                        FIRST_VALUE(population_raw IGNORE NULLS) 
                            OVER(PARTITION BY id_zone ORDER BY year 
                                ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING),
                        0
                    ) as population,
                    
                    CASE WHEN population_raw IS NULL THEN TRUE ELSE FALSE END as is_imputed
                FROM raw_unpivoted
            ),
            stats_calc AS (
                SELECT 
                    *,
                    (population - LAG(population) OVER(PARTITION BY id_zone ORDER BY year)) / 
                    NULLIF(LAG(population) OVER(PARTITION BY id_zone ORDER BY year), 0) as pct_change,
                    
                    (population - AVG(population) OVER(PARTITION BY year)) / 
                    NULLIF(STDDEV(population) OVER(PARTITION BY year), 0) as z_score_size
                FROM calc_imputation
            )
            SELECT 
                *,
                CASE 
                    WHEN ABS(z_score_size) > 10 
                        OR ABS(pct_change) > 2
                    THEN TRUE 
                    ELSE FALSE 
                END as is_atypical
            FROM stats_calc

            """

        sql_logic = f"""
            BEGIN TRANSACTION;
            CREATE TABLE IF NOT EXISTS {target_table} AS {query_transformacion} LIMIT 0;
            
            MERGE INTO {target_table} AS target
                USING ({query_transformacion}) AS source
                ON target.id_zone = source.id_zone 
                AND target.name_zone = source.name_zone
                AND target.year = source.year
                WHEN NOT MATCHED THEN
                    INSERT BY NAME;
            COMMIT;
                """.replace('\n', ' ').strip()
        batch_configs.append({
                        'resourceRequirements': [
                            {'type': 'VCPU', 'value': "4", },
                            {'type': 'MEMORY', 'value': "8110", }
                        ],
                        "environment": [
                            {"name": "SQL_QUERY", "value": sql_logic},
                            {"name": "memory", "value": "7GB"},
                            {"name": "AWS_DEFAULT_REGION", "value": "eu-central-1"},
                            {"name": "USUARIO_POSTGRES", "value": "neondb_owner"},
                            {"name": "CONTR_POSTGRES", "value": pg.password},
                            {"name": "HOST_POSTGRES", "value": pg.host},
                            {"name": "RUTA_S3_DUCKLAKE", "value": "s3://yena-s3-ducklake"}
                        ]
                    })
        return batch_configs
        

    @task()
    def average_income_check():
        
        table_name = "bronze.renta_media"
        target_table = "silver.average_income" # Nombre de la tabla destino
        con = get_db_connection()
        # 1. Detección de columnas (Tu código original)
        cols_df = con.sql(f"DESCRIBE {table_name}").df()
        year_cols = [f"{c}"for c in cols_df['column_name'] if c.endswith('_value')]
        year_cols.sort()
        years_labels = [c.split('_')[0] for c in year_cols]

        sql_col_list = "[" + ", ".join([f'"{c}"' for c in year_cols]) + "]"
        sql_year_label_list = "[" + ", ".join([f"'{y}'" for y in years_labels]) + "]"

        print(f"Detectados años: {years_labels}")


        query_transformacion = f"""
        WITH raw_unpivoted AS (
            SELECT 
                id_zone,
                name as name_zone,
                d.zone_type  as zone_type,
                d.source as source,
                UNNEST({sql_year_label_list})::INT as year,
                UNNEST({sql_col_list})::DOUBLE as income_raw
            FROM {table_name} br LEFT JOIN silver.dim_zones d ON br.ine_district = d.original_id AND d.source = 'ine' AND d.zone_type = 'districts'
        ),
        calc_imputation AS (
            SELECT 
                *,
                COALESCE(
                    income_raw, 
                    LAST_VALUE(income_raw IGNORE NULLS) 
                        OVER(PARTITION BY id_zone ORDER BY year 
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
                    FIRST_VALUE(income_raw IGNORE NULLS) 
                        OVER(PARTITION BY id_zone ORDER BY year 
                            ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING),
                    0
                ) as income,
                CASE WHEN income_raw IS NULL THEN TRUE ELSE FALSE END as is_imputed
            FROM raw_unpivoted
        ),
        stats_calc AS (
            SELECT 
                *,
                (income - LAG(income) OVER(PARTITION BY id_zone ORDER BY year)) / 
                NULLIF(LAG(income) OVER(PARTITION BY id_zone ORDER BY year), 0) as pct_change,
                
                (income - AVG(income) OVER(PARTITION BY year)) / 
                NULLIF(STDDEV(income) OVER(PARTITION BY year), 0) as z_score_size
            FROM calc_imputation
        )
        SELECT 
            *,
            CASE 
                WHEN ABS(z_score_size) > 5  
                    OR ABS(pct_change) > 1 
                THEN TRUE 
                ELSE FALSE 
            END as is_atypical
        FROM stats_calc
        """
        sql_logic = f"""
            BEGIN TRANSACTION;
            CREATE TABLE IF NOT EXISTS {target_table} AS {query_transformacion} LIMIT 0;
            
            MERGE INTO {target_table} AS target
                USING ({query_transformacion}) AS source
                ON target.id_zone = source.id_zone 
                    AND target.name_zone = source.name_zone
                    AND target.year = source.year
                    WHEN NOT MATCHED THEN
                        INSERT BY NAME;
            COMMIT;
                """.replace('\n', ' ').strip()
        batch_configs = []
        batch_configs.append({
                        'resourceRequirements': [
                            {'type': 'VCPU', 'value': "2", },
                            {'type': 'MEMORY', 'value': "8192", }
                        ],
                        "environment": [
                            {"name": "SQL_QUERY", "value": sql_logic},
                            {"name": "memory", "value": "7GB"},
                            {"name": "AWS_DEFAULT_REGION", "value": "eu-central-1"},
                            {"name": "USUARIO_POSTGRES", "value": "neondb_owner"},
                            {"name": "CONTR_POSTGRES", "value": pg.password},
                            {"name": "HOST_POSTGRES", "value": pg.host},
                            {"name": "RUTA_S3_DUCKLAKE", "value": "s3://yena-s3-ducklake"}
                        ]
                    })
        
        return batch_configs


    @task()
    def check_trips_quality():
        con = get_db_connection()
        source_table="silver.od_trips"
        con.sql("""
            -- Cuántas filas tengo y qué fechas cubro
            SELECT
                MIN(date) AS min_date,
                MAX(date) AS max_date,
                COUNT(*)       AS num_rows
            FROM silver.od_trips;

                    """).show()
        print(f"Checking statistical quality from {source_table}...")

        # 1. Definimos la query de análisis
        query = f"""--sql
        WITH base_metrics AS (
            SELECT 
                *,
                -- Métrica derivada clave: Kilómetros promedio por viaje unitario
                -- Si n_trips es 0, evitamos división por cero
                trips_total_length_km / NULLIF(n_trips, 0) as avg_km_per_trip
            FROM {source_table} USING SAMPLE 2% -- dado el gran numero de viajes usamos un sample de los datos
        ),
        stats_window AS (
            SELECT 
                *,
                -- --- ESTADÍSTICAS POR GRUPO DE DISTANCIA ---
                -- Comparamos cada fila contra el comportamiento normal de su rango de distancia
                
                -- A) Para N_TRIPS
                AVG(n_trips) OVER(PARTITION BY distance_group_km) as mean_trips,
                STDDEV(n_trips) OVER(PARTITION BY distance_group_km) as std_trips,
                
                -- B) Para TOTAL LENGTH
                AVG(trips_total_length_km) OVER(PARTITION BY distance_group_km) as mean_len,
                STDDEV(trips_total_length_km) OVER(PARTITION BY distance_group_km) as std_len,
                
                -- C) Para AVG KM PER TRIP (Detecta inconsistencias físicas)
                AVG(avg_km_per_trip) OVER(PARTITION BY distance_group_km) as mean_avg_km,
                STDDEV(avg_km_per_trip) OVER(PARTITION BY distance_group_km) as std_avg_km

            FROM base_metrics
        ),
        z_scores AS (
            SELECT 
                *,
                -- Cálculo de Z-SCORES (Cuantas desviaciones estándar se aleja de la media)
                (n_trips - mean_trips) / NULLIF(std_trips, 0) as z_score_trips,
                (trips_total_length_km - mean_len) / NULLIF(std_len, 0) as z_score_len,
                (avg_km_per_trip - mean_avg_km) / NULLIF(std_avg_km, 0) as z_score_avg_km
            FROM stats_window
        )
        SELECT 
            date,
            zone_type,
            id_origin,
            id_destination,
            distance_group_km,
            n_trips,
            trips_total_length_km,

            ROUND(avg_km_per_trip, 2) as avg_km_per_trip,
            -- Guardamos los Z-Scores para filtrar
            ROUND(z_score_trips, 2) as z_n_trips,
            ROUND(z_score_len, 2) as z_total_km,
            ROUND(z_score_avg_km, 2) as z_avg_km,

            -- --- DIAGNÓSTICO FINAL (FLAGS) ---
            CASE 
                WHEN ABS(z_score_trips) > 20 THEN 'extreme volume'
                WHEN ABS(z_score_avg_km) > 20 THEN 'Inconsistent Distance'
                WHEN avg_km_per_trip > 1000 THEN 'Travel > 1000km'
                WHEN n_trips > 0 AND trips_total_length_km = 0 THEN 'Viajes sin Distancia'
                ELSE 'OK'
            END as quality_flag,
            
            ingestion_date
        FROM z_scores
        -- Opcional: Filtramos solo lo sospechoso para que la tabla auxiliar no sea gigante
        -- WHERE ABS(z_score_trips) > 3 OR ABS(z_score_avg_km) > 3
        """

    
        print("\nSummary atypical trips:")
        summary = con.sql(f"""
            SELECT quality_flag, count(*) as count 
            FROM ({query})
            GROUP BY quality_flag 
            ORDER BY count DESC
        """).show()
        


        # Mostramos ejemplos de errores
        print("\nMost extreme atypical:")
        con.sql(f"""
            SELECT * FROM ({query})
            WHERE quality_flag != 'OK' 
            ORDER BY (ABS(z_avg_km),ABS(z_n_trips), ABS(z_total_km)) DESC LIMIT 10
        """).show()



    task_init = init_schema()

    task_dim = master_zones()
    # run_master_zones = BatchOperator.partial(
    #     task_id='zones_batch',
    #     job_name='master-zones-worker',
    #     job_queue='duck_jobque',
    #     job_definition='duck_jobdef',
    #     region_name='eu-central-1',
        
    # ).expand(container_overrides=task_dim)


    task_create_trips = create_silver_trips()
    #task_load_trips =  load_silver_trips.expand(zone_type = ["Distritos","Municipios", "GAU"],year=[2023], month=[10], day=list(range(1,32,1)))
    #batch_overrides_list = sql_batch(zones = ["Distritos","Municipios", "GAU"],years=[2023], months=[3,4,5,6,7,8,9,11,12], days=list(range(1,32,1)))
    batch_overrides_list = sql_batch(zones = ["Distritos","Municipios","GAU"],years=[2023], months=[1,2], days=list(range(1,32,1)))
    # 3. Lanzar a Batch en paralelo
    process_silver_batch = BatchOperator.partial(
        task_id='silver_trips_batch',
        job_name='silver-trips-worker',
        job_queue='duck_jobque',
        job_definition='duck_jobdef',
        region_name='eu-central-1',

    ).expand(container_overrides=batch_overrides_list)
    

    task_load_zones = load_silver_zone()
    task_load_zones_relations = load_silver_ine_mitma_zones()
    task_load_zone_pairs = load_zone_pairs.expand(zone_type=["districts", "municiples", "gaus"])
    
    task_population = population_check()
    population_silver_batch = BatchOperator.partial(
        task_id='silver_population_batch',
        job_name='silver-population-worker',
        job_queue='duck_jobque',
        job_definition='duck_jobdef',
        region_name='eu-central-1',

    ).expand(container_overrides=task_population)

    task_income = average_income_check()
    income_silver_batch = BatchOperator.partial(
        task_id='silver_income_batch',
        job_name='silver-income-worker',
        job_queue='duck_jobque',
        job_definition='duck_jobdef',
        region_name='eu-central-1',

    ).expand(container_overrides=task_income)
    
    task_trips_check = check_trips_quality()


    task_init >> task_dim
    task_dim >> task_create_trips
    task_create_trips >> batch_overrides_list
    process_silver_batch >> task_trips_check

    task_dim >> task_load_zones
    task_load_zones >> task_load_zone_pairs

    task_dim >> task_load_zones_relations

    task_dim >> task_population
    task_dim >> task_income

# Instanciamos el DAG
mobility_ingestion = silver_mobility_dag()
