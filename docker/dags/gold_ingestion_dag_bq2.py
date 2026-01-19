from airflow.sdk import Asset, dag, task
from airflow.sdk.bases.hook import BaseHook
from airflow.providers.amazon.aws.operators.batch import BatchOperator
import duckdb
import pandas as pd
import numpy as np
import requests
import calendar
import json
from datetime import datetime, timedelta
import os
from pathlib import Path
import xml.etree.ElementTree as ET
import re
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
pg = BaseHook.get_connection("neon_postgres")
aws = BaseHook.get_connection("aws_default")


# Función auxiliar para conectar a DuckDB con las extensiones necesarias


def get_db_connection():

    con = duckdb.connect()
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
    dag_id='ingesta_gold_bq2',
    #schedule_interval='@weekly', # Ejecutar semanalmente o cuando quieras
    #start_date=days_ago(1),
    default_args={'owner': 'airflow','retries': 3,'retry_delay': timedelta(minutes=1)},

    
    catchup=False,
    max_active_tasks=2,
    tags=['master', 'duckdb', 'gold'],
    params = {}
      )

def business2_dag():
    @task
    def gold_schema() -> None:
        con = get_db_connection()
        con.sql("CREATE SCHEMA IF NOT EXISTS gold;")
        con.close()




    @task
    def table_gravity_pair():
        con = get_db_connection()
        con.sql("""--sql
        CREATE TABLE IF NOT EXISTS gold.gravity_pair_features (
            zone_type VARCHAR,
            year INTEGER,
            id_origin INTEGER,
            id_destination INTEGER,
            distance_km DOUBLE,
            actual_trips DOUBLE,
            pop_origin DOUBLE,
            inc_destination DOUBLE,
            x_ij DOUBLE,
            ingestion_date TIMESTAMP
        );""")  

    @task
    def sql_gravity_pair(year, zones):

        batch_configs = [ ]
        for zt in zones:
            query = f"""
                WITH
                
                od AS (SELECT 
                        TRY_CAST(id_origin AS INTEGER) AS id_origin,
                        TRY_CAST(id_destination AS INTEGER) AS id_destination,
                        round(SUM(n_trips),1) AS actual_trips
                    FROM silver.od_trips
                    WHERE zone_type = '{zt}'
                    AND YEAR(date) = {year}
                    AND id_origin <> id_destination
                    GROUP BY 1,2 ),
            
                dist AS (
                SELECT
                        zone_type,
                        id_origin AS a,
                        id_destination AS b,
                        distance_km
                    FROM silver.zone_pairs
                    WHERE zone_type = '{zt}'),

                aux_inc AS(
                SELECT DISTINCT id_districts_ine,
                        id_{zt}_mitma
                FROM silver.ine_mitma_zones
                ),
                inc AS (
                    SELECT 
                        a.id_{zt}_mitma as id_zone,
                        SUM(income) AS inc
                    FROM silver.average_income i
                    LEFT JOIN aux_inc a 
                    ON i.id_zone = a.id_districts_ine 
                    WHERE i.year = {year}
                    AND id_{zt}_mitma NOT NULL
                    GROUP BY 1
                ),

                aux_pop AS(
                SELECT DISTINCT id_sections_ine,
                        id_{zt}_mitma
                FROM silver.ine_mitma_zones
                ),

                pop AS (
                    SELECT 
                        id_{zt}_mitma as id_zone, 
                        SUM(population) as population
                    FROM silver.spain_population p 
                    LEFT JOIN aux_pop a 
                    ON p.id_zone = a.id_sections_ine 
                    WHERE year = {year}
                    AND id_{zt}_mitma NOT NULL
                    GROUP BY 1
                    ),
                
            joined AS(
                SELECT
                        '{zt}' AS zone_type,
                        {year} AS year,
                        o.id_origin,
                        o.id_destination,
                        d.distance_km,
                        o.actual_trips,
                        po.population AS pop_origin,
                        rd.inc AS inc_destination
                    FROM od o
                    LEFT JOIN dist d
                    ON LEAST(o.id_origin, o.id_destination) = d.a
                    AND GREATEST(o.id_origin, o.id_destination) = d.b
                    LEFT JOIN pop po ON o.id_origin = po.id_zone
                    LEFT JOIN inc rd ON o.id_destination = rd.id_zone)
            SELECT
                    zone_type,
                    year,
                    id_origin,
                    id_destination,
                    distance_km,
                    actual_trips,
                    pop_origin,
                    inc_destination,
                    ROUND((pop_origin * inc_destination)/ NULLIF(POWER(GREATEST(distance_km, 0.1), {2}), 0),4) AS x_ij,
                    CURRENT_TIMESTAMP AS ingestion_date
                FROM joined
                WHERE pop_origin IS NOT NULL 
                AND inc_destination IS NOT NULL
                    """
            
            #ql_logic1 = q.replace('\n', ' ').strip()
            sql_logic1 = f"""
                            MERGE INTO gold.gravity_pair_features as target
                            USING ({query} )AS source
                                ON target.zone_type = source.zone_type 
                                AND target.year = source.year
                                AND target.id_origin = source.id_origin
                                AND target.id_destination = source.id_destination
                                WHEN NOT MATCHED THEN
                                    INSERT BY NAME;
                            """.replace('\n', ' ').strip()

            batch_configs.append({
                'resourceRequirements': [
                    {'type': 'VCPU', 'value': "4", },
                    {'type': 'MEMORY', 'value': "16384", }
                ],
                "environment": [
                    {"name": "SQL_QUERY", "value": sql_logic1},
                    {"name": "memory", "value": "15GB"},
                    {"name": "AWS_DEFAULT_REGION", "value": "eu-central-1"},
                    {"name": "USUARIO_POSTGRES", "value": "neondb_owner"},
                    {"name": "CONTR_POSTGRES", "value": pg.password},
                    {"name": "HOST_POSTGRES", "value": pg.host},
                    {"name": "RUTA_S3_DUCKLAKE", "value": "s3://yena-s3-ducklake"}
                ]
            })
        return batch_configs
    @task
    def sql_gravity_pair_distr(year, zt):

        batch_configs = [ ]
        date_str = f"{year}-{3:02d}-{1:02d}"
        date_half= f"{year}-{6:02d}-{14:02d}"
        date_end = f"{year}-{12:02d}-{31:02d}"
        query1 = f"""
            WITH
            od AS (SELECT 
                    TRY_CAST(id_origin AS INTEGER) AS id_origin,
                    TRY_CAST(id_destination AS INTEGER) AS id_destination,
                    round(SUM(n_trips),1) AS actual_trips
                FROM silver.od_trips
                WHERE zone_type = '{zt}'
                AND YEAR(date) = {year}
                AND date >= '{date_str} 00:00:00' 
                AND date < '{date_half} 23:59:59.999'
                AND id_origin <> id_destination
                GROUP BY 1,2 ),
        
            dist AS (
            SELECT
                    zone_type,
                    id_origin AS a,
                    id_destination AS b,
                    distance_km
                FROM silver.zone_pairs
                WHERE zone_type = '{zt}'),

            aux_inc AS(
            SELECT DISTINCT id_districts_ine,
                    id_{zt}_mitma
            FROM silver.ine_mitma_zones
            ),
            inc AS (
                SELECT 
                    a.id_{zt}_mitma as id_zone,
                    SUM(income) AS inc
                FROM silver.average_income i
                LEFT JOIN aux_inc a 
                ON i.id_zone = a.id_districts_ine 
                WHERE i.year = {year}
                AND id_{zt}_mitma NOT NULL
                GROUP BY 1
            ),

            aux_pop AS(
            SELECT DISTINCT id_sections_ine,
                    id_{zt}_mitma
            FROM silver.ine_mitma_zones
            ),

            pop AS (
                SELECT 
                    id_{zt}_mitma as id_zone, 
                    SUM(population) as population
                FROM silver.spain_population p 
                LEFT JOIN aux_pop a 
                ON p.id_zone = a.id_sections_ine 
                WHERE year = {year}
                AND id_{zt}_mitma NOT NULL
                GROUP BY 1
                ),
        joined AS(
            SELECT
                    '{zt}' AS zone_type,
                    {year} AS year,
                    o.id_origin,
                    o.id_destination,
                    d.distance_km,
                    o.actual_trips,
                    po.population AS pop_origin,
                    rd.inc AS inc_destination
                FROM od o
                LEFT JOIN dist d
                ON LEAST(o.id_origin, o.id_destination) = d.a
                AND GREATEST(o.id_origin, o.id_destination) = d.b
                LEFT JOIN pop po ON o.id_origin = po.id_zone
                LEFT JOIN inc rd ON o.id_destination = rd.id_zone)
        SELECT
                zone_type,
                year,
                id_origin,
                id_destination,
                distance_km,
                actual_trips,
                pop_origin,
                inc_destination,
                ROUND((pop_origin * inc_destination)/ NULLIF(POWER(GREATEST(distance_km, 0.1), {2}), 0),4) AS x_ij,
                CURRENT_TIMESTAMP AS ingestion_date
            FROM joined
            WHERE pop_origin IS NOT NULL 
            AND inc_destination IS NOT NULL
                """
        
        #ql_logic1 = q.replace('\n', ' ').strip()
        sql_logic1 = f"""
                        MERGE INTO gold.gravity_pair_features as target
                        USING ({query1} )AS source
                            ON target.zone_type = source.zone_type 
                            AND target.year = source.year
                            AND target.id_origin = source.id_origin
                            AND target.id_destination = source.id_destination
                            WHEN MATCHED THEN UPDATE SET
                                actual_trips = target.actual_trips + source.actual_trips,
                            WHEN NOT MATCHED THEN
                                INSERT BY NAME;
                        """.replace('\n', ' ').strip()

        batch_configs.append({
            'resourceRequirements': [
                {'type': 'VCPU', 'value': "4", },
                {'type': 'MEMORY', 'value': "16384", }
            ],
            "environment": [
                {"name": "SQL_QUERY", "value": sql_logic1},
                {"name": "memory", "value": "15GB"},
                {"name": "AWS_DEFAULT_REGION", "value": "eu-central-1"},
                {"name": "USUARIO_POSTGRES", "value": "neondb_owner"},
                {"name": "CONTR_POSTGRES", "value": pg.password},
                {"name": "HOST_POSTGRES", "value": pg.host},
                {"name": "RUTA_S3_DUCKLAKE", "value": "s3://yena-s3-ducklake"}
            ]
        })

        query2 = f"""
            WITH
            od AS (SELECT 
                    TRY_CAST(id_origin AS INTEGER) AS id_origin,
                    TRY_CAST(id_destination AS INTEGER) AS id_destination,
                    round(SUM(n_trips),1) AS actual_trips
                FROM silver.od_trips
                WHERE zone_type = '{zt}'
                AND YEAR(date) = {year}
                AND date >= '{date_half} 00:00:00' 
                AND date < '{date_end} 23:59:59.999'
                AND id_origin <> id_destination
                GROUP BY 1,2 ),
        
            dist AS (
            SELECT
                    zone_type,
                    id_origin AS a,
                    id_destination AS b,
                    distance_km
                FROM silver.zone_pairs
                WHERE zone_type = '{zt}'),

            aux_inc AS(
            SELECT DISTINCT id_districts_ine,
                    id_{zt}_mitma
            FROM silver.ine_mitma_zones
            ),
            inc AS (
                SELECT 
                    a.id_{zt}_mitma as id_zone,
                    SUM(income) AS inc
                FROM silver.average_income i
                LEFT JOIN aux_inc a 
                ON i.id_zone = a.id_districts_ine 
                WHERE i.year = {year}
                AND id_{zt}_mitma NOT NULL
                GROUP BY 1
            ),

            aux_pop AS(
            SELECT DISTINCT id_sections_ine,
                    id_{zt}_mitma
            FROM silver.ine_mitma_zones
            ),

            pop AS (
                SELECT 
                    id_{zt}_mitma as id_zone, 
                    SUM(population) as population
                FROM silver.spain_population p 
                LEFT JOIN aux_pop a 
                ON p.id_zone = a.id_sections_ine 
                WHERE year = {year}
                AND id_{zt}_mitma NOT NULL
                GROUP BY 1
                ),
        joined AS(
            SELECT
                    '{zt}' AS zone_type,
                    {year} AS year,
                    o.id_origin,
                    o.id_destination,
                    d.distance_km,
                    o.actual_trips,
                    po.population AS pop_origin,
                    rd.inc AS inc_destination
                FROM od o
                LEFT JOIN dist d
                ON LEAST(o.id_origin, o.id_destination) = d.a
                AND GREATEST(o.id_origin, o.id_destination) = d.b
                LEFT JOIN pop po ON o.id_origin = po.id_zone
                LEFT JOIN inc rd ON o.id_destination = rd.id_zone)
        SELECT
                zone_type,
                year,
                id_origin,
                id_destination,
                distance_km,
                actual_trips,
                pop_origin,
                inc_destination,
                ROUND((pop_origin * inc_destination)/ NULLIF(POWER(GREATEST(distance_km, 0.1), {2}), 0),4) AS x_ij,
                CURRENT_TIMESTAMP AS ingestion_date
            FROM joined
            WHERE pop_origin IS NOT NULL 
            AND inc_destination IS NOT NULL
                """
        
        
        sql_logic2= f"""
                        MERGE INTO gold.gravity_pair_features as target
                        USING ({query2} )AS source
                            ON target.zone_type = source.zone_type 
                            AND target.year = source.year
                            AND target.id_origin = source.id_origin
                            AND target.id_destination = source.id_destination
                            WHEN MATCHED THEN UPDATE SET
                                actual_trips = target.actual_trips + source.actual_trips,
                            WHEN NOT MATCHED THEN
                                INSERT BY NAME;
                        """.replace('\n', ' ').strip()

        batch_configs.append({
            'resourceRequirements': [
                {'type': 'VCPU', 'value': "4", },
                {'type': 'MEMORY', 'value': "16384", }
            ],
            "environment": [
                {"name": "SQL_QUERY", "value": sql_logic2},
                {"name": "memory", "value": "15GB"},
                {"name": "AWS_DEFAULT_REGION", "value": "eu-central-1"},
                {"name": "USUARIO_POSTGRES", "value": "neondb_owner"},
                {"name": "CONTR_POSTGRES", "value": pg.password},
                {"name": "HOST_POSTGRES", "value": pg.host},
                {"name": "RUTA_S3_DUCKLAKE", "value": "s3://yena-s3-ducklake"}
            ]
        })
        return batch_configs
    

    def _label_clusters(df_days: pd.DataFrame) -> dict[int, str]:
        df_days = df_days.copy()
        df_days["is_weekday"] = df_days["weekday"] <= 4

        stats = (
            df_days.groupby("cluster_id")
            .agg(
                weekday_rate=("is_weekday", "mean"),
                avg_total_trips=("total_trips", "mean"),
                avg_morning=("morning_share", "mean"),
                avg_evening=("evening_share", "mean"),
                n_days=("trip_date", "count"),
            )
            .sort_values(["weekday_rate", "avg_total_trips"], ascending=[True, True])
        )

        cids = list(stats.index)
        label_map: dict[int, str] = {}

        if len(cids) == 1:
            label_map[cids[0]] = "Single pattern"
            return label_map

        if len(cids) == 2:
            label_map[cids[0]] = "Weekend"
            label_map[cids[1]] = "Weekday"
            return label_map

        label_map[cids[0]] = "Weekend"
        label_map[cids[-1]] = "Weekday"
        for cid in cids[1:-1]:
            label_map[cid] = "Holiday"

        return label_map

    @task
    def fit_gravity_k(zones, year):
        
        con = get_db_connection()
        con.sql("""
            CREATE TABLE IF NOT EXISTS gold.gravity_params (
                zone_type VARCHAR,
                year INTEGER,
                k DOUBLE,
                n_pairs_used BIGINT,
                fitted_at TIMESTAMP
            );
        """)
        #con.sql(f"DELETE FROM gold.gravity_params WHERE zone_type='{zt}' AND year={year};")
        for zt in zones:
            query= f"""
                        WITH bounds AS (
                SELECT 
                    quantile_cont(x_ij, 0.25) as lower_limit,
                    quantile_cont(x_ij, 0.75) as upper_limit  
                FROM gold.gravity_pair_features
                WHERE zone_type='{zt}' AND year={year}
                AND x_ij IS NOT NULL AND x_ij > 0
                AND actual_trips IS NOT NULL
            )

            SELECT
                '{zt}' AS zone_type,
                {year} AS year,
            
                SUM(t.x_ij * t.actual_trips) / NULLIF(SUM(t.x_ij * t.x_ij), 0) AS k,
                COUNT(*) AS n_pairs_used,
                CURRENT_TIMESTAMP AS fitted_at
            FROM gold.gravity_pair_features t, bounds b
            WHERE t.zone_type='{zt}' AND t.year={year}
                AND t.x_ij IS NOT NULL 
                AND t.actual_trips IS NOT NULL

                AND t.x_ij >= b.lower_limit 
                AND t.x_ij <= b.upper_limit
                
                """
            
            sql_logic2= f"""
                            MERGE INTO gold.gravity_params as target
                            USING ({query} )AS source
                                ON target.zone_type = source.zone_type 
                                AND target.year = source.year
                                AND target.n_pairs_used = source.n_pairs_used
                                WHEN NOT MATCHED THEN
                                    INSERT BY NAME;
                            """.replace('\n', ' ').strip()
            con.sql(sql_logic2)
        # batch_configs = []
        # batch_configs.append({
        #     'resourceRequirements': [
        #         {'type': 'VCPU', 'value': "4", },
        #         {'type': 'MEMORY', 'value': "16384", }
        #     ],
        #     "environment": [
        #         {"name": "SQL_QUERY", "value": sql_logic2},
        #         {"name": "memory", "value": "15GB"},
        #         {"name": "AWS_DEFAULT_REGION", "value": "eu-central-1"},
        #         {"name": "USUARIO_POSTGRES", "value": "neondb_owner"},
        #         {"name": "CONTR_POSTGRES", "value": pg.password},
        #         {"name": "HOST_POSTGRES", "value": pg.host},
        #         {"name": "RUTA_S3_DUCKLAKE", "value": "s3://yena-s3-ducklake"}
        #     ]
        # })
        print(f"[BQ2] gold.gravity_params fitted for zone_type={zt}, year={year}.")
        #con.sql(f"SELECT * FROM gold.gravity_params WHERE zone_type='{zt}' AND year={year};").show()

    @task
    def infrastructure_gap_sql(year = 2023):

        batch_configs = []
        con = get_db_connection()
        con.sql("""
        CREATE TABLE IF NOT EXISTS gold.infrastructure_gaps (
            zone_type VARCHAR,
            year INTEGER,
            id_origin INTEGER,
            id_destination INTEGER,
            distance_km DOUBLE,
            actual_trips DOUBLE,
            theoretical_trips DOUBLE,
            mismatch_ratio DOUBLE,
            gap DOUBLE,
            ingestion_date TIMESTAMP
        );
    """)
        query = f"""WITH kpar AS (
            SELECT zone_type, k
            FROM gold.gravity_params
            WHERE year={year}
        )
        SELECT
            f.zone_type,
            f.year,
            f.id_origin,
            f.id_destination,
            f.distance_km,
            f.actual_trips,
            (k.k * f.x_ij) AS theoretical_trips,
            f.actual_trips / NULLIF((k.k * f.x_ij), 0) AS mismatch_ratio,
            GREATEST((k.k * f.x_ij) - f.actual_trips, 0) AS gap,
            CURRENT_TIMESTAMP AS ingestion_date
        FROM gold.gravity_pair_features f
        INNER JOIN kpar k
            ON f.zone_type = k.zone_type
        WHERE f.year={year}
          AND f.x_ij IS NOT NULL AND f.x_ij > 0"""

        sql_logic = f"""
                BEGIN TRANSACTION;
                MERGE INTO  gold.infrastructure_gaps as target
                USING ({query}) AS sc
                    ON target.year = sc.year
                    AND target.zone_type = sc.zone_type
                    AND target.id_origin = sc.id_origin
                    AND target.id_destination = sc.id_destination
                    AND target.distance_km = sc.distance_km
                    WHEN NOT MATCHED THEN
                        INSERT BY NAME;
                COMMIT;""".replace('\n', ' ').strip()

        batch_configs.append({
            'resourceRequirements': [
                {'type': 'VCPU', 'value': "4", },
                {'type': 'MEMORY', 'value': "16384", }
            ],
            "environment": [
                {"name": "SQL_QUERY", "value": sql_logic},
                {"name": "memory", "value": "15GB"},
                {"name": "AWS_DEFAULT_REGION", "value": "eu-central-1"},
                {"name": "USUARIO_POSTGRES", "value": "neondb_owner"},
                {"name": "CONTR_POSTGRES", "value": pg.password},
                {"name": "HOST_POSTGRES", "value": pg.host},
                {"name": "RUTA_S3_DUCKLAKE", "value": "s3://yena-s3-ducklake"}
            ]
        })
        return batch_configs

    gold_init = gold_schema()
    table_gravity = table_gravity_pair()
    batchoverride_insert_gravity = sql_gravity_pair(year=2023, zones=["gaus","municiples"])

    batch_insert_gravity = BatchOperator.partial(
        task_id='gold-batch-gravity_raw',
        job_name='gold-batch-gravity_raw',
        job_queue='duck_jobque',
        job_definition='duck_jobdef',
        region_name='eu-central-1',
        submit_job_timeout= 600,

        
    ).expand(container_overrides=batchoverride_insert_gravity)

    #batchoverride_insert_gravity_distr = sql_gravity_pair_distr(year=2023, zt="districts")

    # batch_insert_gravity_distr = BatchOperator.partial(
    #     task_id='gold-batch-gravity_raw_distr',
    #     job_name='gold-batch-gravity_raw_distr',
    #     job_queue='duck_jobque',
    #     job_definition='duck_jobdef',
    #     region_name='eu-central-1',
    #     submit_job_timeout= 600,
    # ).expand(container_overrides=batchoverride_insert_gravity_distr)

    fit_k =  fit_gravity_k(zones=["gaus","municiples","districts"], year=2023)

    infa_gap = infrastructure_gap_sql(year=2023)
    batch_infrastructure_gap = BatchOperator.partial(
        task_id='batch_infrastructure',
        job_name='infa_gap',
        job_queue='duck_jobque',
        job_definition='duck_jobdef',
        region_name='eu-central-1',
        submit_job_timeout= 600,
    ).expand(container_overrides=infa_gap)

    gold_init >> table_gravity
    table_gravity >> batchoverride_insert_gravity
    #table_gravity >> batchoverride_insert_gravity_distr
    batch_insert_gravity >> fit_k
    fit_k >> infa_gap




gold1 = business2_dag()
