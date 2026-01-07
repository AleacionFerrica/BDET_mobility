import duckdb
import pandas as pd
import requests
import json
from datetime import datetime, timedelta
import os
from pathlib import Path
import xml.etree.ElementTree as ET
import re

def create_master_zones(con):
    # secuencia (empezamos en 1)
    con.sql("""
        CREATE TABLE IF NOT EXISTS silver.dim_zones (
            id_zone INTEGER,             -- Entero simple (lo calcularemos nosotros)
            original_id VARCHAR,
            zone_type VARCHAR,
            source VARCHAR,
            ingestion_date TIMESTAMP
        );
    """)
    
    # Índice para velocidad (opcional pero recomendado)
    # Nota: Si DuckLake no soporta índices, puedes omitir esta línea
    try:
        con.sql("CREATE UNIQUE INDEX IF NOT EXISTS idx_original_id ON silver.dim_zones (original_id);")
    except:
        pass # Si falla por limitaciones del Lake, seguimos igual
        
    print("Master Table 'silver.dim_zones' .")

def update_master_zones(con):
    print("Searching new zone codes...")

    # 1. Definimos la query de los candidatos (igual que antes)
    # Buscamos todos los códigos que existen en Bronze
    query_candidatos = """--sql
        WITH gaus_info AS (SELECT * FROM bronze.gaus_info 
                    WHERE id_gaus != 'NA'
                      AND id_gaus IS NOT NULL),

            districts_info AS (SELECT * FROM bronze.districts_info  
                    WHERE id_districts != 'NA' 
                    AND id_districts IS NOT NULL),

            municipless_info AS (SELECT * FROM bronze.municiples_info 
                    WHERE id_municiples != 'NA' 
                    AND id_municiples IS NOT NULL),

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
            SELECT TRIM(origen) as original_id, CASE WHEN zone_type  = 'GAU' THEN 'gaus' WHEN zone_type  = 'Distritos' THEN 'districts' WHEN zone_type  = 'Municipios' THEN 'municiples' ELSE zone_type  END as zone_type, 'mitma' as source FROM bronze.trips
            UNION ALL
            SELECT TRIM(destino) as original_id,CASE WHEN zone_type  = 'GAU' THEN 'gaus' WHEN zone_type  = 'Distritos' THEN 'districts' WHEN zone_type  = 'Municipios' THEN 'municiples' ELSE zone_type END as zone_type, 'mitma' as source FROM bronze.trips
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

    # INSERT 
    #  a) Calculamos el MAX(id_zone) actual (si es NULL, usamos 0)
    #  b) A los nuevos les sumamos ROW_NUMBER() + ese MAX
    
    con.sql(f"""--sql
        INSERT INTO silver.dim_zones (id_zone, original_id, zone_type, source, ingestion_date) 
        
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
            (SELECT max_current_id FROM current_state) + ROW_NUMBER() OVER(ORDER BY nc.original_id) as new_sk,
            
            nc.original_id,
            nc.zone_type,
            nc.source,
            current_timestamp
        FROM new_codes_to_insert nc
    """)
    





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

            CASE WHEN br.zone_type = 'Distritos' THEN 'distritcs' WHEN br.zone_type = 'Municipios' THEN 'municiples' 
                WHEN br.zone_type ='GAU' THEN 'gaus' END as zone_type,

            d_o.id_zone AS id_origin,

            d_d.id_zone AS id_destination,


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
        LEFT JOIN silver.dim_zones d_o
            ON br.origen = d_o.original_id AND d_o.source = 'mitma' AND d_o.zone_type = '{zone_dic[zone_type]}'
        LEFT JOIN silver.dim_zones d_d 
            ON br.destino = d_d.original_id AND d_d.source = 'mitma' AND d_d.zone_type = '{zone_dic[zone_type]}'
        WHERE 
            -- FILTROS NUMÉRICOS (Ignoran ceros a la izquierda o espacios)
            --try_strptime(date::VARCHAR ), '%Y%m%d%H') as date 
            YEAR(try_strptime(fecha::VARCHAR , '%Y%m%d')) = {year}
            AND MONTH(try_strptime(fecha::VARCHAR, '%Y%m%d')) = {month}
            AND TRIM(br.zone_type) = '{zone_type}'


    """
    con.sql(query)


def load_silver_ine_mitma_zones(con):
   
    q = f""" --sql
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

    con.sql(f"CREATE OR REPLACE TABLE silver.ine_mitma_zones AS {q} ")

def load_silver_zone(con):
    #zone_type = "districts"
    q = """ --sql
    WITH gaus AS (SELECT 
                id_zone,
                ifnull(TRIM(br.name_gaus), 'external') AS name_zone,
                br.zone_type,
                CASE  -- comprueba si el codigo viene de fuera de españa en caso lo pone como EXT
                    WHEN br.id_gaus LIKE 'FR%' 
                    OR br.id_gaus LIKE 'PT%' 
                    OR br.id_gaus LIKE 'IT%' 
                    OR br.id_gaus LIKE 'DE%' 
                    OR br.id_gaus LIKE 'UK%' 
                    OR br.id_gaus LIKE 'US%'
                    OR br.id_gaus LIKE 'EXT%'
                    THEN 'EXT'
                    ELSE 'ESP'
                END AS country_zone,
                geometry,
                centroid,
                ST_PointOnSurface(br.geometry) AS visual_point,
            FROM bronze.gaus_info br
            LEFT JOIN silver.dim_zones d
            ON br.id_gaus = d.original_id AND br.zone_type = d.zone_type AND d.source = 'mitma'
            WHERE id_gaus != 'NA' AND id_gaus IS NOT NULL 
    ),
    municiples AS (SELECT 
                id_zone,
                ifnull(TRIM(br.name_municiples), 'external') AS name_zone,
                br.zone_type,
                CASE  -- comprueba si el codigo viene de fuera de españa en caso lo pone como EXT
                    WHEN br.id_municiples LIKE 'FR%' 
                    OR br.id_municiples LIKE 'PT%' 
                    OR br.id_municiples LIKE 'IT%' 
                    OR br.id_municiples LIKE 'DE%' 
                    OR br.id_municiples LIKE 'UK%' 
                    OR br.id_municiples LIKE 'US%'
                    OR br.id_municiples LIKE 'EXT%'
                    THEN 'EXT'
                    ELSE 'ESP'
                END AS country_zone,
                geometry,
                centroid,
                ST_PointOnSurface(br.geometry) AS visual_point,
            FROM bronze.municiples_info br
            LEFT JOIN silver.dim_zones d
            ON br.id_municiples = d.original_id AND br.zone_type = d.zone_type AND d.source = 'mitma'
            WHERE id_municiples != 'NA' AND id_municiples IS NOT NULL 
    ),
    districts AS (SELECT 
                id_zone,
                ifnull(TRIM(br.name_districts), 'external') AS name_zone,
                br.zone_type,
                CASE  -- comprueba si el codigo viene de fuera de españa en caso lo pone como EXT
                    WHEN br.id_districts LIKE 'FR%' 
                    OR br.id_districts LIKE 'PT%' 
                    OR br.id_districts LIKE 'IT%' 
                    OR br.id_districts LIKE 'DE%' 
                    OR br.id_districts LIKE 'UK%' 
                    OR br.id_districts LIKE 'US%'
                    OR br.id_districts LIKE 'EXT%'
                    THEN 'EXT'
                    ELSE 'ESP'
                END AS country_zone,
                geometry,
                centroid,
                ST_PointOnSurface(br.geometry) AS visual_point,
            FROM bronze.districts_info br
            LEFT JOIN silver.dim_zones d
            ON br.id_districts = d.original_id AND br.zone_type = d.zone_type AND d.source = 'mitma' 
            WHERE id_districts != 'NA' AND id_districts IS NOT NULL 
    )

    SELECT * FROM  gaus
    UNION ALL 
    SELECT * FROM  municiples
    UNION ALL 
    SELECT * FROM  districts


    """
    con.sql(f"CREATE OR REPLACE TABLE silver.zones_info AS(SELECT *, current_timestamp as ingestion_date FROM ({q})) ")


def load_zone_pairs(con, zone_type):
    
    pairs_query=f"""--sql
                    WITH base AS (
                            SELECT
                                zone_type,
                                id_zone,
                                centroid
                            FROM silver.zones_info WHERE country_zone = 'ESP' AND zone_type = '{zone_type}'--solo nos interesan zonas dentro de españa 
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
    con.sql(f"""SELECT 
        SUM("2021_value") as total_2021, 
        SUM("2022_value") as total_2022, 
        SUM("2023_value") as total_2023, 

        
        
        FROM bronze.poblacion_total """)
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
            id_zone, -- Usamos Sección Censal como ID
            section_name,
            UNNEST({sql_year_label_list})::INT as year,
            UNNEST({sql_col_list})::DOUBLE as population_raw
        FROM {table_name} br LEFT JOIN silver.dim_zones d ON br.ine_section = d.original_id AND d.source = 'ine' AND d.zone_type = 'sections'
    ),
    calc_imputation AS (
        SELECT 
            *,
            -- IMPUTACIÓN (Forward Fill + Backward Fill + 0)
            COALESCE(
                population_raw, 
                
                -- Si falta dato, coge el del año anterior
                LAST_VALUE(population_raw IGNORE NULLS) 
                    OVER(PARTITION BY id_zone ORDER BY year 
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
                
                --  Si es el primer año y falta, coge el del siguiente
                FIRST_VALUE(population_raw IGNORE NULLS) 
                    OVER(PARTITION BY id_zone ORDER BY year 
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
            (population - LAG(population) OVER(PARTITION BY id_zone ORDER BY year)) / 
            NULLIF(LAG(population) OVER(PARTITION BY id_zone ORDER BY year), 0) as pct_change,
            
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
            id_zone,
            name,
            UNNEST({sql_year_label_list})::INT as year,
            UNNEST({sql_col_list})::DOUBLE as rent_raw
        FROM {table_name} br LEFT JOIN silver.dim_zones d ON br.ine_district = d.original_id AND d.source = 'ine' AND d.zone_type = 'districts'
    ),
    calc_imputation AS (
        SELECT 
            *,
            COALESCE(
                rent_raw, 
                LAST_VALUE(rent_raw IGNORE NULLS) 
                    OVER(PARTITION BY id_zone ORDER BY year 
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
                FIRST_VALUE(rent_raw IGNORE NULLS) 
                    OVER(PARTITION BY id_zone ORDER BY year 
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
            (rent - LAG(rent) OVER(PARTITION BY id_zone ORDER BY year)) / 
            NULLIF(LAG(rent) OVER(PARTITION BY id_zone ORDER BY year), 0) as pct_change,
            
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
    --ORDER BY ine_district, year
    """

    con.sql(f"CREATE OR REPLACE TABLE {target_table} AS {query_transformacion}")

    n_atipicos = con.sql(f"SELECT count(*) FROM {target_table} WHERE is_atypical = TRUE").fetchone()[0]
    print(f"Marked {n_atipicos} observations as atypical.")

def check_trips_quality(con):
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
            ATTACH 'ducklake:mobility.ducklake' AS my_ducklake;
            USE my_ducklake;
            CREATE SCHEMA IF NOT EXISTS silver;
                """)
    create_master_zones(con)
    update_master_zones(con)

    load_silver_ine_mitma_zones(con)
    con.sql("DROP TABLE silver.od_trips")
    load_silver_trips(con, 2023,zone_type="Distritos")
    load_silver_trips(con, 2023,zone_type="GAU")
    load_silver_trips(con, 2023,zone_type="Municipios")
    load_silver_zone(con)
    load_zone_pairs(con, zone_type="districts")
    load_zone_pairs(con, zone_type="gaus")

    population_check(con)
    average_rent_check(con)
    check_trips_quality(con)
