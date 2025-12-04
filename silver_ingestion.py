import duckdb
import pandas as pd
import requests
import json
from datetime import datetime, timedelta
import os
from pathlib import Path
import xml.etree.ElementTree as ET
import re

def map_gaus(con):
    print("Generando tabla de mapeo para códigos GAU...")
    
    # 1. Definimos la lógica
    # Extraemos todos los códigos únicos que empiezan por 'GAU' de tu tabla bronze
    # Asignamos un número empezando por 90001 (para evitar coincidir con CP o INE)
    
    mapping_query = """--sql
    WITH unique_gaus AS (
        SELECT DISTINCT 
            TRIM(id_gaus) as mitma_id -- Quitamos espacios por seguridad
        FROM bronze.gaus_info
        WHERE id_gaus LIKE 'GAU%'
    )
    SELECT 
        mitma_id,
        CAST((abs(hash(mitma_id)) % 100000) + 900000 AS VARCHAR)as internal_id
    FROM unique_gaus
    """


    con.sql(f"""
        CREATE OR REPLACE TABLE silver.gau_id_mapping AS 
        {mapping_query}
    """)

    print("Tabla 'silver.gau_id_mapping' creada.")


def load_silver_trips(con, year, zone_type, month=None):
    print(f"Loading Silver for {zone_type} {month}/{year}...")
    zone_dic = {"Municipios":"municiples","Distritos":"districts","GAU":"gaus"}

    con.sql("""--sql
        CREATE TABLE IF NOT EXISTS  silver.od_trips (
            date TIMESTAMP,
            --hour_period INTEGER,
            zone_type VARCHAR,
            -- od
            id_origin VARCHAR,
            id_destination VARCHAR,
            origin_activity VARCHAR,
            destination_activity VARCHAR,
            
            distance_group_km VARCHAR,
            residence_province VARCHAR,
            -- dem groups
            rent_group VARCHAR,
            age_group VARCHAR,
            sex_group VARCHAR,

            --distance
            n_trips DOUBLE,
            trips_total_length_km DOUBLE,

            origin_activity_std boolean,
            destination_activity_std boolean,

            ingestion_date TIMESTAMP
        );
    """)
    con.sql("ALTER TABLE silver.od_trips SET PARTITIONED BY (zone_type, YEAR(date), MONTH(date) );")



    # Borramos datos previos de esa partición para evitar duplicados
    con.sql(f"""
        DELETE FROM silver.od_trips
        WHERE zone_type = '{zone_dic[zone_type]}'
        AND YEAR(date) = {year}
        AND MONTH(date) = {month}
    """)
    
    query = f"""--sql
    INSERT INTO silver.od_trips
    SELECT 
            try_strptime(fecha::VARCHAR || LPAD(periodo::VARCHAR, 2, '0'), '%Y%m%d%H') as date,

            CASE WHEN zone_type = 'Distritos' THEN 'distritcs' WHEN zone_type = 'Municipios' THEN 'municiples' 
                WHEN zone_type ='GAU' THEN 'gaus' END as origin_activity,

            COALESCE(
                mapo.internal_id,              -- 1. Intenta usar el ID del mapa (si era GAU texto)
                TRIM(CAST(br.origen AS VARCHAR)) -- 2. Si no cruzó, convierte el original a número
                ) AS id_origin,

            COALESCE(
                    mapd.internal_id, 
                    TRIM(CAST(br.destino AS VARCHAR))
                ) AS id_destination,


            CASE WHEN actividad_origen  = 'casa' THEN 'Home' WHEN actividad_origen  = 'trabajo_estudio' THEN 'Work_Study'  
                WHEN actividad_origen  ='frecuente' THEN 'frequent'  ELSE 'not_frequent' END as origin_activity,

            CASE WHEN actividad_destino  = 'casa' THEN 'Home' WHEN actividad_destino  = 'trabajo_estudio' THEN 'Work_Study'  
                WHEN actividad_destino  ='frecuente' THEN 'frequent'  ELSE 'not_frequent' END as destination_activity,
                
                    -- distance,
            TRIM(TRY_CAST(distancia AS VARCHAR)) as distnace_groups_km,
                
            SUBSTR(residencia, 1, 2) as residence_province, --substring extracts first 2 characters starting from the first1 character
            TRY_CAST(NULLIF(renta, 'NA') AS VARCHAR) as rent_group,
            TRY_CAST(NULLIF(edad , 'NA')AS VARCHAR) as age_group ,
            CASE WHEN sexo = 'hombre' THEN 'male' WHEN sexo = 'mujer' THEN 'female' ELSE 'NULL' END as sex_group,

            ROUND(TRY_CAST(viajes AS DOUBLE), 1) as n_trips, 
            ROUND(TRY_CAST(viajes_km AS DOUBLE), 3) as trips_total_length_km,

            CASE WHEN estudio_origen_posible = 'no' THEN 'False' WHEN estudio_origen_posible = 'si' THEN 'True' END as origin_activity_std,

            CASE WHEN estudio_destino_posible = 'no' THEN 'False' WHEN estudio_destino_posible = 'si' THEN 'True' END as destination_activity_std,

            CURRENT_TIMESTAMP AS ingestion_date

        FROM bronze.trips br 
        LEFT JOIN silver.gau_id_mapping AS mapo ON br.origen = mapo.mitma_id
        LEFT JOIN silver.gau_id_mapping AS mapd ON br.destino = mapd.mitma_id
        WHERE 
            -- FILTROS NUMÉRICOS (Ignoran ceros a la izquierda o espacios)
            --try_strptime(date::VARCHAR ), '%Y%m%d%H') as date 
            YEAR(try_strptime(fecha::VARCHAR , '%Y%m%d')) = {year}
            AND MONTH(try_strptime(fecha::VARCHAR, '%Y%m%d')) = {month}
            AND TRIM(zone_type) = '{zone_type}'

    """
    con.sql(query)


def load_silver_ine_mitma_zones(con):
    q=f""" 
        SELECT 
        TRIM(NULLIF(seccion_ine , 'NA')) as sections_ine,
        TRIM(distrito_ine) as districts_ine,
        TRIM(municipio_ine) as municiples_ine,
        
        TRIM(distrito_mitma) as districts_mitma,
        TRIM(municipio_mitma) as municiples_mitma,
        COALESCE(m.internal_id,TRIM(br.gau_mitma)) as gau_mitma,

        current_timestamp as ingestion_date
        FROM bronze.ine_mitma_zones AS br LEFT JOIN silver.gau_id_mapping AS m ON br.gau_mitma = m.mitma_id
        WHERE NULLIF(seccion_ine , 'NA') NOT NULL
        """
    con.sql(f"CREATE OR REPLACE TABLE silver.ine_mitma_zones AS {q} ")

def load_silver_zone(con,zone_type):
    q = f""" --sql
    SELECT
          COALESCE(m.internal_id,br.id_{zone_type}) as id_zone, --COALESCE elije uno de los dos argumentos que no sea nulo
          ifnull(TRIM(br.name_{zone_type}), 'external') AS name,
          CASE  -- comprueba si el codigo viene de fuera de españa en caso lo pone como EXT
                WHEN br.id_{zone_type} LIKE 'FR%' 
                OR br.id_{zone_type} LIKE 'PT%' 
                OR br.id_{zone_type} LIKE 'IT%' 
                OR br.id_{zone_type} LIKE 'DE%' 
                OR br.id_{zone_type} LIKE 'UK%' 
                OR br.id_{zone_type} LIKE 'US%'
                OR br.id_{zone_type} LIKE 'EXT%'
                THEN 'EXT'
                ELSE 'ESP'
                END AS country_zone,
                br.geometry as geometry,
                br.centroid as centroid,
                ST_PointOnSurface(br.geometry) AS visual_point,
                CURRENT_TIMESTAMP       AS ingestion_date
        FROM bronze.{zone_type}_info AS br LEFT JOIN silver.gau_id_mapping AS m ON br.id_{zone_type} = m.mitma_id """
    con.sql(f"CREATE OR REPLACE TABLE silver.{zone_type}_zone AS {q} ")


def load_zone_pairs(con, zone_type):
    
    pairs_query=f"""--sql
                    WITH base AS (
                            SELECT
                                '{zone_type}' AS zone_type,
                                TRIM(id_zone)   AS id_zone,
                                centroid
                            FROM silver.{zone_type}_zone WHERE country_zone = 'ESP' --solo nos interesan zonas dentro de españa 
                                )
                        
                        SELECT
                            a.zone_type,
                            a.id_zone AS id_origin,
                            b.id_zone AS id_destination,
                            ROUND(ST_Distance(a.centroid, b.centroid) / 1000.0,3) AS distance_km --unidades en km

                        FROM base AS a
                        CROSS JOIN base AS b -- cross join para tener los pares 
                        WHERE a.id_zone < b.id_zone -- con < nos aseguramos de no tener distancia de A-B y B-A ni A-A   
                          """


    con.sql(f"""--sql
        CREATE TABLE IF NOT EXISTS silver.zone_pairs AS {pairs_query} LIMIT 0
    """)
    con.sql(f"""
        DELETE FROM silver.zone_pairs 
        WHERE zone_type = '{zone_type}'
      
    """)
    con.sql("ALTER TABLE silver.zone_pairs SET PARTITIONED BY (zone_type)")

    con.sql(f""" --sql
            INSERT INTO silver.zone_pairs BY NAME
            SELECT * FROM ({pairs_query})  
            
            """)

def population_check(con):
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

    # 2. Query Maestra: Unpivot + Imputación + Estadísticas + Detección de Atípicos
    query_transformacion = f"""--sql
    WITH raw_unpivoted AS (
        SELECT 
            ine_section, -- Usamos Sección Censal como ID
            name,
            UNNEST({sql_year_label_list})::INT as year,
            UNNEST({sql_col_list})::DOUBLE as population_raw
        FROM {table_name}
    ),
    calc_imputation AS (
        SELECT 
            *,
            -- IMPUTACIÓN (Forward Fill + Backward Fill + 0)
            COALESCE(
                population_raw, 
                
                -- Si falta dato, coge el del año anterior
                LAST_VALUE(population_raw IGNORE NULLS) 
                    OVER(PARTITION BY ine_section ORDER BY year 
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
                
                --  Si es el primer año y falta, coge el del siguiente
                FIRST_VALUE(population_raw IGNORE NULLS) 
                    OVER(PARTITION BY ine_section ORDER BY year 
                        ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING),
                
                -- Si todo es nulo, asume 0 habitantes
                0
            ) as population,
            
            CASE WHEN population_raw IS NULL THEN TRUE ELSE FALSE END as is_imputed
        FROM raw_unpivoted
    ),
    stats_calc AS (
        SELECT 
            *,
            -- Variación Interanual
            (population - LAG(population) OVER(PARTITION BY ine_section ORDER BY year)) / 
            NULLIF(LAG(population) OVER(PARTITION BY ine_section ORDER BY year), 0) as pct_change,
            
            -- Z-Score (Comparación con el resto de secciones ese año)
            (population - AVG(population) OVER(PARTITION BY year)) / 
            NULLIF(STDDEV(population) OVER(PARTITION BY year), 0) as z_score_size
        FROM calc_imputation
    )
    SELECT 
        *,
        -- --- DETECCIÓN DE ATÍPICOS ---
        -- Marca TRUE si es un outlier estadístico extremo Y ADEMÁS ha tenido un cambio brusco
        CASE 
            WHEN ABS(z_score_size) > 10 
                OR ABS(pct_change) > 2
            THEN TRUE 
            ELSE FALSE 
        END as is_atypical
    FROM stats_calc

    """

    con.sql(f"CREATE OR REPLACE TABLE {target_table} AS {query_transformacion}")


    n_atipicos = con.sql(f"SELECT count(*) FROM {target_table} WHERE is_atypical = TRUE").fetchone()[0]
    print(f"Marked {n_atipicos} observations as atypical.")

def average_rent_check(con):
    table_name = "bronze.renta_media"
    target_table = "silver.average_rent" # Nombre de la tabla destino

    # 1. Detección de columnas (Tu código original)
    cols_df = con.sql(f"DESCRIBE {table_name}").df()
    year_cols = [f"{c}"for c in cols_df['column_name'] if c.endswith('_value')]
    year_cols.sort()
    years_labels = [c.split('_')[0] for c in year_cols]

    sql_col_list = "[" + ", ".join([f'"{c}"' for c in year_cols]) + "]"
    sql_year_label_list = "[" + ", ".join([f"'{y}'" for y in years_labels]) + "]"

    print(f"Detectados años: {years_labels}")

    # 2. Query Maestra con lógica de Atípicos añadida
    query_transformacion = f"""--sql
    WITH raw_unpivoted AS (
        SELECT 
            ine_district,
            name,
            UNNEST({sql_year_label_list})::INT as year,
            UNNEST({sql_col_list})::DOUBLE as rent_raw
        FROM {table_name}
    ),
    calc_imputation AS (
        SELECT 
            *,
            COALESCE(
                rent_raw, 
                LAST_VALUE(rent_raw IGNORE NULLS) 
                    OVER(PARTITION BY ine_district ORDER BY year 
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
                FIRST_VALUE(rent_raw IGNORE NULLS) 
                    OVER(PARTITION BY ine_district ORDER BY year 
                        ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING),
                0
            ) as rent,
            CASE WHEN rent_raw IS NULL THEN TRUE ELSE FALSE END as is_imputed
        FROM raw_unpivoted
    ),
    stats_calc AS (
        SELECT 
            *,
            -- Cálculo de variación (Change)
            (rent - LAG(rent) OVER(PARTITION BY ine_district ORDER BY year)) / 
            NULLIF(LAG(rent) OVER(PARTITION BY ine_district ORDER BY year), 0) as pct_change,
            
            -- Cálculo de Z-Score
            (rent - AVG(rent) OVER(PARTITION BY year)) / 
            NULLIF(STDDEV(rent) OVER(PARTITION BY year), 0) as z_score_size
        FROM calc_imputation
    )
    SELECT 
        *,
        -- --- LÓGICA DE DETECCIÓN DE ATÍPICOS ---
        -- Condición: (Z-Score > 5 O < -5) Y (Cambio fuera de rango -1 a 1)
        CASE 
            WHEN ABS(z_score_size) > 5  -- Esto cubre > 5 y < -5
                OR ABS(pct_change) > 1 -- Esto cubre > 1 (100%) y < -1 (-100%)
            THEN TRUE 
            ELSE FALSE 
        END as is_atypical
    FROM stats_calc
    ORDER BY ine_district, year
    """

    con.sql(f"CREATE OR REPLACE TABLE {target_table} AS {query_transformacion}")

    n_atipicos = con.sql(f"SELECT count(*) FROM {target_table} WHERE is_atypical = TRUE").fetchone()[0]
    print(f"Marked {n_atipicos} observations as atypical.")

def check_trips_quality(con):
    source_table="silver.od_trips"

    print(f"Checking statistical quality from {source_table}...")

    # 1. Definimos la query de análisis
    query = f"""--sql
    WITH base_metrics AS (
        SELECT 
            *,
            -- Métrica derivada clave: Kilómetros promedio por viaje unitario
            -- Si n_trips es 0, evitamos división por cero
            trips_total_length_km / NULLIF(n_trips, 0) as avg_km_per_trip
        FROM {source_table} USING SAMPLE 5% -- dado el gran numero de viajes usamos un sample de los datos
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


if __name__ == "__main__":
    con = duckdb.connect()
    con.sql("""INSTALL ducklake; LOAD ducklake;
                INSTALL spatial; LOAD spatial;
    """)
    con.sql(f"""
            ATTACH 'ducklake:mobility_ducklake.ducklake' AS my_ducklake;
            USE my_ducklake;
            CREATE SCHEMA IF NOT EXISTS silver;
                """)
    map_gaus(con)
    load_silver_ine_mitma_zones(con)
    #con.sql("DROP TABLE silver.od_trips")
    load_silver_trips(con, 2023, month= 6,zone_type="Distritos")
    load_silver_trips(con, 2023, month= 6,zone_type="GAU")
    load_silver_zone(con,zone_type="districts")
    load_silver_zone(con,zone_type="gaus")
    load_zone_pairs(con, zone_type="districts")
    load_zone_pairs(con, zone_type="gaus")

    population_check(con)
    average_rent_check(con)
    check_trips_quality(con)
