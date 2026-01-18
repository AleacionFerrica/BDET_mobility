from airflow.sdk import Asset, dag, task, Param
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
    dag_id='ingesta_gold',
    #schedule_interval='@weekly', # Ejecutar semanalmente o cuando quieras
    #start_date=days_ago(1),
    default_args={'owner': 'airflow','retries': 3,'retry_delay': timedelta(minutes=1)},

    
    catchup=False,
    max_active_tasks=10,
    tags=['master', 'duckdb', 'gold'] )

def business1_dag():
    @task
    def gold_schema() -> None:
        con = get_db_connection()
        con.sql("CREATE SCHEMA IF NOT EXISTS gold;")
        con.close()

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
    def aux_table_sql():
        zones = zones=["gaus","municiples","districts"]
        batch_configs = []
        for zt in zones:
            query = f"""
                CREATE TABLE IF NOT EXISTS gold.aux_{zt} AS
                    SELECT
                        CAST(date AS DATE) AS trip_date,
                        zone_type,
                        EXTRACT(HOUR FROM date) AS hour_of_day,
                        SUM(n_trips) AS total_trips
                    FROM silver.od_trips
                    WHERE zone_type = '{zt}'
                    AND date >= '2023-01-01 00:00:00' 
                    AND date < '2023-12-31 23:59:59.999'
                    GROUP BY 1,2,3
                    ORDER BY 1,2,3;
                    """
            batch_configs.append({
                    'resourceRequirements': [
                        {'type': 'VCPU', 'value': "4", },
                        {'type': 'MEMORY', 'value': "32768", }
                    ],
                    "environment": [
                        {"name": "SQL_QUERY", "value": query},
                        {"name": "memory", "value": "31GB"},
                            {"name": "AWS_DEFAULT_REGION", "value": "eu-central-1"},
                            {"name": "USUARIO_POSTGRES", "value": "neondb_owner"},
                            {"name": "CONTR_POSTGRES", "value": pg.password},
                            {"name": "HOST_POSTGRES", "value": pg.host},
                            {"name": "RUTA_S3_DUCKLAKE", "value": "s3://yena-s3-ducklake"}
                        ]
                    })
                    
            
        return batch_configs

    #CLUSTERS: k-means
    @task
    def build_day_clusters(
        year: int,
        n_clusters: int = 3,
        include_volume_in_clustering: bool = True,
        random_state: int = 42) -> None:


        zone_types = ["gaus","municiples","districts"  ]

        if not zone_types:
            print(f"[BQ1] No zone_types found for year={year}.")
            return

        zone_list_sql = ", ".join([f"'{z}'" for z in zone_types])
        
        for zt in zone_types:
            con = get_db_connection()
            df_temporal = con.sql(
                f"""
                SELECT * FROM gold.aux_{zt}
                """
            ).df()

            print(f"df_temp created for {zt}")

            if df_temporal.empty:
                print(f"[BQ1] No data in silver.od_trips for year={year}.")
                return

            pivot = (
                df_temporal.pivot_table(
                    index=["trip_date", "zone_type"],
                    columns="hour_of_day",
                    values="total_trips",
                    aggfunc="sum",
                    fill_value=0.0,
                )
                .sort_index()
            )
            pivot = pivot.reindex(columns=list(range(24)), fill_value=0.0)
            #print(pivot)
            daily_total = pivot.sum(axis=1)
            shares = pivot.div(daily_total.replace(0, np.nan), axis=0).fillna(0.0)

            peak_hour = pivot.idxmax(axis=1).astype(int)
            
            morning_share = shares.loc[:, 7:10].sum(axis=1)
            evening_share = shares.loc[:, 17:20].sum(axis=1)

            df_features = shares.copy()
            df_features.columns = [f"share_h{h:02d}" for h in df_features.columns]
            df_features = df_features.reset_index()

            df_features["total_trips"] = daily_total.values
            df_features["peak_hour"] = peak_hour.values
            df_features["morning_share"] = morning_share.values
            df_features["evening_share"] = evening_share.values
            df_features["weekday"] = pd.to_datetime(df_features["trip_date"]).dt.weekday
            #print(df_features)
            share_cols = [c for c in df_features.columns if c.startswith("share_h")]

            out_rows: list[pd.DataFrame] = []

        
            df_zt = df_features[df_features["zone_type"] == zt].copy()
            if df_zt.empty:
                continue

            n_days = len(df_zt)
            k = min(n_clusters, n_days)
            if k < 2:
                df_zt["cluster_id"] = 0
                df_zt["pattern_name"] = "Single pattern (insufficient days)"
                out_rows.append(df_zt[["trip_date", "zone_type", "cluster_id", "pattern_name"]])
                continue

            X_parts = [df_zt[share_cols].to_numpy(dtype=float)]
            if include_volume_in_clustering:
                vol = np.log1p(df_zt["total_trips"].to_numpy(dtype=float)).reshape(-1, 1)
                pk = df_zt["peak_hour"].to_numpy(dtype=float).reshape(-1, 1)
                X_parts += [vol, pk]

            X = np.hstack(X_parts)
            X_scaled = StandardScaler().fit_transform(X)

            km = KMeans(n_clusters=k, random_state=random_state, n_init=10)
            df_zt["cluster_id"] = km.fit_predict(X_scaled)

            label_map = _label_clusters(df_zt)
            df_zt["pattern_name"] = df_zt["cluster_id"].map(label_map).fillna(
                "Pattern " + df_zt["cluster_id"].astype(str)
            )

            out_rows.append(df_zt[["trip_date", "zone_type", "cluster_id", "pattern_name"]])
            print(f"[BQ1] zone_type={zt}: {n_days} days clustered into k={k}")

            df_clusters = pd.concat(out_rows, ignore_index=True)


            #con.sql("DROP TABLE gold.day_clusters ")
            con.sql("CREATE TABLE IF NOT EXISTS gold.day_clusters AS SELECT *,current_timestamp as ingestion_date FROM df_clusters LIMIT 0 ;")

            con.sql(
                """
                BEGIN TRANSACTION;
                MERGE INTO  gold.day_clusters as target
                USING (SELECT *, current_timestamp as ingestion_date
                        FROM df_clusters) AS sc
                        ON target.trip_date = sc.trip_date
                        AND target.zone_type = sc.zone_type
                        WHEN NOT MATCHED THEN
                            INSERT BY NAME;
                COMMIT;
                """)

        print("[BQ1] gold.day_clusters created. Days per cluster:")
        con.sql(
            """
            SELECT zone_type, cluster_id, pattern_name, COUNT(*) AS n_days
            FROM gold.day_clusters
            GROUP BY 1,2,3
            ORDER BY zone_type, n_days DESC
            """
        ).show()
        #return df_clusters
    @task()
    def create_staging():
        con = get_db_connection()
        # CREATE OR REPLACE PARA CADA VEZ QUE SE EJECUTE BORRAR la TABLA, si la dejamos entera cada vez aumentara infinitamente los valores acumulados 
        con.sql("""
        CREATE TABLE IF NOT EXISTS gold.staging_accumulated (
        zone_type VARCHAR,
        cluster_id INTEGER,
        pattern_name VARCHAR,
        hour_of_day INTEGER,
        id_origin INTEGER,
        id_destination INTEGER,
        distance_group_km VARCHAR,
        
        -- Acumuladores
        sum_daily_trips DOUBLE DEFAULT 0,
        sum_daily_length_km DOUBLE DEFAULT 0,
        days_count INTEGER DEFAULT 0
        )
        """)
        con.sql("ALTER TABLE gold.staging_accumulated SET PARTITIONED BY (zone_type, cluster_id);")
        con.close()

    @task()
    def build_staging_accumulated_sql(zones, years, months):
        
        batch_configs = []
        for zone in zones:
            for year in years:
                for month in months:
                    _, last_day = calendar.monthrange(year, month)
                    date_str = f"{year}-{month:02d}-{1:02d}"
                    date_half= f"{year}-{month:02d}-{14:02d}"
                     #ESTO ES UNA ESTU^PIDEZ PERO DISTRICTS TODO EL MES NO LO CARGA NI PA ATRAS
                    date_end = f"{year}-{month:02d}-{last_day:02d}"
                    # ESTO GUARDA EN LA tabla staging por zona cluster hora origen, destino y grupo de distancia el numero de viajes para luego hacer el promedio de viajes
                    source_query1 = f"""
                    WITH daily_stats AS (
                        SELECT 
                            CAST(t.date AS DATE) AS trip_date,
                            t.zone_type,
                            EXTRACT(HOUR FROM t.date) AS hour_of_day,
                            TRY_CAST(t.id_origin AS INTEGER) AS id_origin,
                            TRY_CAST(t.id_destination AS INTEGER) AS id_destination,
                            t.distance_group_km,
                            
                            SUM(t.n_trips) AS daily_trips,
                            SUM(t.trips_total_length_km) AS daily_total_length_km,

                        FROM silver.od_trips t
                        WHERE t.zone_type = '{zone}'
                        AND date >= '{date_str} 00:00:00' 
                        AND date < '{date_half} 23:59:59.999'
                        GROUP BY 1,2,3,4,5,6
                        HAVING id_origin IS NOT NULL AND id_destination IS NOT NULL
                            ),
                        monthly_aggregate AS (
                            SELECT 
                                ds.zone_type,
                                dc.cluster_id,
                                dc.pattern_name,
                                ds.hour_of_day,
                                ds.id_origin,
                                ds.id_destination,
                                ds.distance_group_km,
                                
                                ROUND(SUM(ds.daily_trips),2) AS sum_daily_trips,
                                ROUND(SUM(ds.daily_total_length_km),2) AS sum_daily_length_km,
                                COUNT(DISTINCT ds.trip_date) AS days_count
                            FROM daily_stats ds
                            LEFT JOIN gold.day_clusters dc 
                            ON ds.trip_date = dc.trip_date 
                            AND ds.zone_type = dc.zone_type
                            GROUP BY 1,2,3,4,5,6,7
                        )
                        SELECT * FROM monthly_aggregate
                    """
                    sql_logic1 = f"""
                    BEGIN TRANSACTION;
                    MERGE INTO gold.staging_accumulated AS target
                    USING ({source_query1}) AS source
                    ON target.zone_type = source.zone_type
                    AND target.cluster_id = source.cluster_id
                    AND target.pattern_name = source.pattern_name
                    AND target.hour_of_day = source.hour_of_day
                    AND target.id_origin = source.id_origin
                    AND target.id_destination = source.id_destination
                    AND target.distance_group_km = source.distance_group_km
                    WHEN MATCHED THEN UPDATE SET
                        sum_daily_trips = target.sum_daily_trips + source.sum_daily_trips,
                        sum_daily_length_km = target.sum_daily_length_km + source.sum_daily_length_km,
                        days_count = target.days_count + source.days_count
                    WHEN NOT MATCHED THEN INSERT BY NAME;
                    COMMIT;
                    """.replace('\n', ' ').strip()

                    batch_configs.append({
                    'resourceRequirements': [
                        {'type': 'VCPU', 'value': "4", },
                        {'type': 'MEMORY', 'value': "32768", }
                    ],
                    "environment": [
                        {"name": "SQL_QUERY", "value": sql_logic1},
                        {"name": "memory", "value": "30GB"},
                            {"name": "AWS_DEFAULT_REGION", "value": "eu-central-1"},
                            {"name": "USUARIO_POSTGRES", "value": "neondb_owner"},
                            {"name": "CONTR_POSTGRES", "value": pg.password},
                            {"name": "HOST_POSTGRES", "value": pg.host},
                            {"name": "RUTA_S3_DUCKLAKE", "value": "s3://yena-s3-ducklake"}
                        ]
                    })
                    source_query2 = f"""
                    WITH daily_stats  AS (
                        SELECT 
                            CAST(t.date AS DATE) AS trip_date,
                            t.zone_type,
                            EXTRACT(HOUR FROM t.date) AS hour_of_day,
                            TRY_CAST(t.id_origin AS INTEGER) AS id_origin,
                            TRY_CAST(t.id_destination AS INTEGER) AS id_destination,
                            t.distance_group_km,
                            
                            SUM(t.n_trips) AS daily_trips,
                            SUM(t.trips_total_length_km) AS daily_total_length_km,

                        FROM silver.od_trips t
                        WHERE t.zone_type = '{zone}'
                        AND date >= '{date_half} 00:00:00' 
                        AND date < '{date_end} 23:59:59.999'
                        GROUP BY 1,2,3,4,5,6
                        HAVING id_origin IS NOT NULL AND id_destination IS NOT NULL
                            ),
                        monthly_aggregate AS (
                            SELECT 
                                ds.zone_type,
                                dc.cluster_id,
                                dc.pattern_name,
                                ds.hour_of_day,
                                ds.id_origin,
                                ds.id_destination,
                                ds.distance_group_km,
                                
                                ROUND(SUM(ds.daily_trips),2) AS sum_daily_trips,
                                ROUND(SUM(ds.daily_total_length_km),2) AS sum_daily_length_km,
                                COUNT(DISTINCT ds.trip_date) AS days_count
                            FROM daily_stats ds
                            LEFT JOIN gold.day_clusters dc 
                            ON ds.trip_date = dc.trip_date 
                            AND ds.zone_type = dc.zone_type
                            GROUP BY 1,2,3,4,5,6,7
                        )
                        SELECT * FROM monthly_aggregate
                    """
                    sql_logic2 = f"""
                    BEGIN TRANSACTION;
                    MERGE INTO gold.staging_accumulated AS target
                    USING ({source_query2}) AS source
                    ON target.zone_type = source.zone_type
                    AND target.cluster_id = source.cluster_id
                    AND target.pattern_name = source.pattern_name
                    AND target.hour_of_day = source.hour_of_day
                    AND target.id_origin = source.id_origin
                    AND target.id_destination = source.id_destination
                    AND target.distance_group_km = source.distance_group_km
                    WHEN MATCHED THEN UPDATE SET
                        sum_daily_trips = target.sum_daily_trips + source.sum_daily_trips,
                        sum_daily_length_km = target.sum_daily_length_km + source.sum_daily_length_km,
                        days_count = target.days_count + source.days_count
                    WHEN NOT MATCHED THEN INSERT BY NAME;
                    COMMIT;
                    """.replace('\n', ' ').strip()

                    batch_configs.append({
                    'resourceRequirements': [
                        {'type': 'VCPU', 'value': "4", },
                        {'type': 'MEMORY', 'value': "32768", }
                    ],
                    "environment": [
                        {"name": "SQL_QUERY", "value": sql_logic2},
                        {"name": "memory", "value": "30GB"},
                            {"name": "AWS_DEFAULT_REGION", "value": "eu-central-1"},
                            {"name": "USUARIO_POSTGRES", "value": "neondb_owner"},
                            {"name": "CONTR_POSTGRES", "value": pg.password},
                            {"name": "HOST_POSTGRES", "value": pg.host},
                            {"name": "RUTA_S3_DUCKLAKE", "value": "s3://yena-s3-ducklake"}
                        ]
                    })
                    
            
        return batch_configs
    @task()
    def build_staging_accumulated_sql_dist(zone, years, months):
        
        batch_configs = []

        for year in years:
            for month in months:
                _, last_day = calendar.monthrange(year, month)
                date_str = f"{year}-{month:02d}-{1:02d}"
                date_half= f"{year}-{month:02d}-{7:02d}"
                date_mid = f"{year}-{month:02d}-{14:02d}" #ESTO ES UNA ESTU^PIDEZ PERO DISTRICTS TODO EL MES NO LO CARGA NI PA ATRAS
                date_mid_half = f"{year}-{month:02d}-{22:02d}"
                date_end = f"{year}-{month:02d}-{last_day:02d}"
                # ESTO GUARDA EN LA tabla staging por zona cluster hora origen, destino y grupo de distancia el numero de viajes para luego hacer el promedio de viajes
                source_query1 = f"""
                WITH daily_stats AS (
                    SELECT 
                        CAST(t.date AS DATE) AS trip_date,
                        t.zone_type,
                        EXTRACT(HOUR FROM t.date) AS hour_of_day,
                        TRY_CAST(t.id_origin AS INTEGER) AS id_origin,
                        TRY_CAST(t.id_destination AS INTEGER) AS id_destination,
                        t.distance_group_km,
                        
                        SUM(t.n_trips) AS daily_trips,
                        SUM(t.trips_total_length_km) AS daily_total_length_km,

                    FROM silver.od_trips t
                    WHERE t.zone_type = '{zone}'
                    AND date >= '{date_str} 00:00:00' 
                    AND date < '{date_half} 23:59:59.999'
                    GROUP BY 1,2,3,4,5,6
                    HAVING id_origin IS NOT NULL AND id_destination IS NOT NULL
                        ),
                    monthly_aggregate AS (
                        SELECT 
                            ds.zone_type,
                            dc.cluster_id,
                            dc.pattern_name,
                            ds.hour_of_day,
                            ds.id_origin,
                            ds.id_destination,
                            ds.distance_group_km,
                            
                            ROUND(SUM(ds.daily_trips),2) AS sum_daily_trips,
                            ROUND(SUM(ds.daily_total_length_km),2) AS sum_daily_length_km,
                            COUNT(DISTINCT ds.trip_date) AS days_count
                        FROM daily_stats ds
                        LEFT JOIN gold.day_clusters dc 
                        ON ds.trip_date = dc.trip_date 
                        AND ds.zone_type = dc.zone_type
                        GROUP BY 1,2,3,4,5,6,7
                    )
                    SELECT * FROM monthly_aggregate
                """
                sql_logic1 = f"""
                MERGE INTO gold.staging_accumulated AS target
                USING ({source_query1}) AS source
                ON target.zone_type = source.zone_type
                AND target.cluster_id = source.cluster_id
                AND target.pattern_name = source.pattern_name
                AND target.hour_of_day = source.hour_of_day
                AND target.id_origin = source.id_origin
                AND target.id_destination = source.id_destination
                AND target.distance_group_km = source.distance_group_km
                WHEN MATCHED THEN UPDATE SET
                    sum_daily_trips = target.sum_daily_trips + source.sum_daily_trips,
                    sum_daily_length_km = target.sum_daily_length_km + source.sum_daily_length_km,
                    days_count = target.days_count + source.days_count
                WHEN NOT MATCHED THEN INSERT BY NAME;
                """.replace('\n', ' ').strip()

                batch_configs.append({
                    'resourceRequirements': [
                        {'type': 'VCPU', 'value': "4", },
                        {'type': 'MEMORY', 'value': "32768", }
                    ],
                    "environment": [
                        {"name": "SQL_QUERY", "value": sql_logic2},
                        {"name": "memory", "value": "30GB"},
                        {"name": "AWS_DEFAULT_REGION", "value": "eu-central-1"},
                        {"name": "USUARIO_POSTGRES", "value": "neondb_owner"},
                        {"name": "CONTR_POSTGRES", "value": pg.password},
                        {"name": "HOST_POSTGRES", "value": pg.host},
                        {"name": "RUTA_S3_DUCKLAKE", "value": "s3://yena-s3-ducklake"}
                    ]
                })
                source_query2 = f"""
                WITH daily_stats  AS (
                    SELECT 
                        CAST(t.date AS DATE) AS trip_date,
                        t.zone_type,
                        EXTRACT(HOUR FROM t.date) AS hour_of_day,
                        TRY_CAST(t.id_origin AS INTEGER) AS id_origin,
                        TRY_CAST(t.id_destination AS INTEGER) AS id_destination,
                        t.distance_group_km,
                        
                        SUM(t.n_trips) AS daily_trips,
                        SUM(t.trips_total_length_km) AS daily_total_length_km,

                    FROM silver.od_trips t
                    WHERE t.zone_type = '{zone}'
                    AND date >= '{date_half} 00:00:00' 
                    AND date < '{date_mid} 23:59:59.999'
                    GROUP BY 1,2,3,4,5,6
                    HAVING id_origin IS NOT NULL AND id_destination IS NOT NULL
                        ),
                    monthly_aggregate AS (
                        SELECT 
                            ds.zone_type,
                            dc.cluster_id,
                            dc.pattern_name,
                            ds.hour_of_day,
                            ds.id_origin,
                            ds.id_destination,
                            ds.distance_group_km,
                            
                            ROUND(SUM(ds.daily_trips),2) AS sum_daily_trips,
                            ROUND(SUM(ds.daily_total_length_km),2) AS sum_daily_length_km,
                            COUNT(DISTINCT ds.trip_date) AS days_count
                        FROM daily_stats ds
                        LEFT JOIN gold.day_clusters dc 
                        ON ds.trip_date = dc.trip_date 
                        AND ds.zone_type = dc.zone_type
                        GROUP BY 1,2,3,4,5,6,7
                    )
                    SELECT * FROM monthly_aggregate
                """
                sql_logic2 = f"""
                MERGE INTO gold.staging_accumulated AS target
                USING ({source_query2}) AS source
                ON target.zone_type = source.zone_type
                AND target.cluster_id = source.cluster_id
                AND target.pattern_name = source.pattern_name
                AND target.hour_of_day = source.hour_of_day
                AND target.id_origin = source.id_origin
                AND target.id_destination = source.id_destination
                AND target.distance_group_km = source.distance_group_km
                WHEN MATCHED THEN UPDATE SET
                    sum_daily_trips = target.sum_daily_trips + source.sum_daily_trips,
                    sum_daily_length_km = target.sum_daily_length_km + source.sum_daily_length_km,
                    days_count = target.days_count + source.days_count
                WHEN NOT MATCHED THEN INSERT BY NAME;
                """.replace('\n', ' ').strip()

                batch_configs.append({
                    'resourceRequirements': [
                        {'type': 'VCPU', 'value': "4", },
                        {'type': 'MEMORY', 'value': "32768", }
                    ],
                    "environment": [
                        {"name": "SQL_QUERY", "value": sql_logic2},
                        {"name": "memory", "value": "30GB"},
                        {"name": "AWS_DEFAULT_REGION", "value": "eu-central-1"},
                        {"name": "USUARIO_POSTGRES", "value": "neondb_owner"},
                        {"name": "CONTR_POSTGRES", "value": pg.password},
                        {"name": "HOST_POSTGRES", "value": pg.host},
                        {"name": "RUTA_S3_DUCKLAKE", "value": "s3://yena-s3-ducklake"}
                    ]
                })
                source_query3 = f"""
                WITH daily_stats AS (
                    SELECT 
                        CAST(t.date AS DATE) AS trip_date,
                        t.zone_type,
                        EXTRACT(HOUR FROM t.date) AS hour_of_day,
                        TRY_CAST(t.id_origin AS INTEGER) AS id_origin,
                        TRY_CAST(t.id_destination AS INTEGER) AS id_destination,
                        t.distance_group_km,
                        
                        SUM(t.n_trips) AS daily_trips,
                        SUM(t.trips_total_length_km) AS daily_total_length_km,

                    FROM silver.od_trips t
                    WHERE t.zone_type = '{zone}'
                    AND date >= '{date_mid} 00:00:00' 
                    AND date < '{date_mid_half} 23:59:59.999'
                    GROUP BY 1,2,3,4,5,6
                    HAVING id_origin IS NOT NULL AND id_destination IS NOT NULL
                        ),
                    monthly_aggregate AS (
                        SELECT 
                            ds.zone_type,
                            dc.cluster_id,
                            dc.pattern_name,
                            ds.hour_of_day,
                            ds.id_origin,
                            ds.id_destination,
                            ds.distance_group_km,
                            
                            ROUND(SUM(ds.daily_trips),2) AS sum_daily_trips,
                            ROUND(SUM(ds.daily_total_length_km),2) AS sum_daily_length_km,
                            COUNT(DISTINCT ds.trip_date) AS days_count
                        FROM daily_stats ds
                        LEFT JOIN gold.day_clusters dc 
                        ON ds.trip_date = dc.trip_date 
                        AND ds.zone_type = dc.zone_type
                        GROUP BY 1,2,3,4,5,6,7
                    )
                    SELECT * FROM monthly_aggregate
                """
                sql_logic3 = f"""
                MERGE INTO gold.staging_accumulated AS target
                USING ({source_query3}) AS source
                ON target.zone_type = source.zone_type
                AND target.cluster_id = source.cluster_id
                AND target.pattern_name = source.pattern_name
                AND target.hour_of_day = source.hour_of_day
                AND target.id_origin = source.id_origin
                AND target.id_destination = source.id_destination
                AND target.distance_group_km = source.distance_group_km
                WHEN MATCHED THEN UPDATE SET
                    sum_daily_trips = target.sum_daily_trips + source.sum_daily_trips,
                    sum_daily_length_km = target.sum_daily_length_km + source.sum_daily_length_km,
                    days_count = target.days_count + source.days_count
                WHEN NOT MATCHED THEN INSERT BY NAME;
                """.replace('\n', ' ').strip()

                batch_configs.append({
                    'resourceRequirements': [
                        {'type': 'VCPU', 'value': "4", },
                        {'type': 'MEMORY', 'value': "32768", }
                    ],
                    "environment": [
                        {"name": "SQL_QUERY", "value": sql_logic3},
                        {"name": "memory", "value": "30GB"},
                        {"name": "AWS_DEFAULT_REGION", "value": "eu-central-1"},
                        {"name": "USUARIO_POSTGRES", "value": "neondb_owner"},
                        {"name": "CONTR_POSTGRES", "value": pg.password},
                        {"name": "HOST_POSTGRES", "value": pg.host},
                        {"name": "RUTA_S3_DUCKLAKE", "value": "s3://yena-s3-ducklake"}
                    ]
                })
                source_query4 = f"""
                WITH daily_stats AS (
                    SELECT 
                        CAST(t.date AS DATE) AS trip_date,
                        t.zone_type,
                        EXTRACT(HOUR FROM t.date) AS hour_of_day,
                        TRY_CAST(t.id_origin AS INTEGER) AS id_origin,
                        TRY_CAST(t.id_destination AS INTEGER) AS id_destination,
                        t.distance_group_km,
                        
                        SUM(t.n_trips) AS daily_trips,
                        SUM(t.trips_total_length_km) AS daily_total_length_km,

                    FROM silver.od_trips t
                    WHERE t.zone_type = '{zone}'
                    AND date >= '{date_mid_half} 00:00:00' 
                    AND date < '{date_end} 23:59:59.999'
                    GROUP BY 1,2,3,4,5,6
                    HAVING id_origin IS NOT NULL AND id_destination IS NOT NULL
                        ),
                    monthly_aggregate AS (
                        SELECT 
                            ds.zone_type,
                            dc.cluster_id,
                            dc.pattern_name,
                            ds.hour_of_day,
                            ds.id_origin,
                            ds.id_destination,
                            ds.distance_group_km,
                            
                            ROUND(SUM(ds.daily_trips),2) AS sum_daily_trips,
                            ROUND(SUM(ds.daily_total_length_km),2) AS sum_daily_length_km,
                            COUNT(DISTINCT ds.trip_date) AS days_count
                        FROM daily_stats ds
                        LEFT JOIN gold.day_clusters dc 
                        ON ds.trip_date = dc.trip_date 
                        AND ds.zone_type = dc.zone_type
                        GROUP BY 1,2,3,4,5,6,7
                    )
                    SELECT * FROM monthly_aggregate
                """
                sql_logic4 = f"""
                MERGE INTO gold.staging_accumulated AS target
                USING ({source_query4}) AS source
                ON target.zone_type = source.zone_type
                AND target.cluster_id = source.cluster_id
                AND target.pattern_name = source.pattern_name
                AND target.hour_of_day = source.hour_of_day
                AND target.id_origin = source.id_origin
                AND target.id_destination = source.id_destination
                AND target.distance_group_km = source.distance_group_km
                WHEN MATCHED THEN UPDATE SET
                    sum_daily_trips = target.sum_daily_trips + source.sum_daily_trips,
                    sum_daily_length_km = target.sum_daily_length_km + source.sum_daily_length_km,
                    days_count = target.days_count + source.days_count
                WHEN NOT MATCHED THEN INSERT BY NAME;
                """.replace('\n', ' ').strip()

                batch_configs.append({
                    'resourceRequirements': [
                        {'type': 'VCPU', 'value': "4", },
                        {'type': 'MEMORY', 'value': "32768", }
                    ],
                    "environment": [
                        {"name": "SQL_QUERY", "value": sql_logic4},
                        {"name": "memory", "value": "30GB"},
                        {"name": "AWS_DEFAULT_REGION", "value": "eu-central-1"},
                        {"name": "USUARIO_POSTGRES", "value": "neondb_owner"},
                        {"name": "CONTR_POSTGRES", "value": pg.password},
                        {"name": "HOST_POSTGRES", "value": pg.host},
                        {"name": "RUTA_S3_DUCKLAKE", "value": "s3://yena-s3-ducklake"}
                    ]
                })

            
        return batch_configs
    
    @task 
    def typical_day_patterns_sql(zones):
        batch_configs = []
        for zt in zones:
            source_query_half = f"""
                
                    WITH 
                        day_counts AS (
                        SELECT 
                            zone_type, 
                            cluster_id, 
                            pattern_name, 
                            COUNT(*) AS n_days
                        FROM gold.day_clusters
                        WHERE zone_type = '{zt}'
                        AND trip_date >= '2023-01-01 00:00:00' 
                        AND trip_date < '2023-12-31 23:59:59.999'
                        GROUP BY 1, 2, 3
                                ),

                    trip_aggregates AS (
                                SELECT 
                                    t.zone_type,
                                    dc.cluster_id,
                                    dc.pattern_name,
                                    EXTRACT(HOUR FROM t.date) AS hour_of_day,
                                    TRY_CAST(t.id_origin AS INTEGER) AS id_origin,
                                    TRY_CAST(t.id_destination AS INTEGER) AS id_destination,
                                    t.distance_group_km,
                                    
                                    SUM(t.n_trips) AS total_trips_period,
                                    
                                FROM silver.od_trips t

                                INNER JOIN gold.day_clusters dc 
                                    ON CAST(t.date AS DATE) = dc.trip_date 
                                    AND t.zone_type = dc.zone_type

                                WHERE t.zone_type = '{zt}'
                                AND t.date >= '2023-01-01 00:00:00' 
                                AND t.date < '2023-06-30 23:59:59.999'
                                
                                GROUP BY 1, 2, 3, 4, 5, 6, 7
                            )
                    
                    SELECT 
                        agg.zone_type,
                        agg.cluster_id,
                        agg.pattern_name,
                        agg.hour_of_day,
                        agg.id_origin,
                        agg.id_destination,
                        agg.distance_group_km,
                        ROUND(agg.total_trips_period / days.n_days, 2) AS avg_trips_per_day,
                        days.n_days AS n_days_in_cluster,
                        current_timestamp as ingestion_date
                    FROM trip_aggregates agg
                    INNER JOIN day_counts days 
                        ON agg.cluster_id = days.cluster_id 
                        AND agg.zone_type = days.zone_type
                        AND agg.pattern_name = days.pattern_name
                    """
            sql_logic = f"""

                MERGE INTO gold.typical_day_patterns2 AS target
                USING ({source_query_half}) AS source
                ON target.zone_type = source.zone_type
                AND target.cluster_id = source.cluster_id
                AND target.hour_of_day = source.hour_of_day
                AND target.id_origin = source.id_origin
                AND target.id_destination = source.id_destination
                AND target.distance_group_km = source.distance_group_km
                WHEN MATCHED THEN
                    UPDATE SET
                    avg_trips_per_day = target.avg_trips_per_day + source.avg_trips_per_day,
                WHEN NOT MATCHED THEN INSERT BY NAME;
                """.replace('\n', ' ').strip()

            batch_configs.append({
                'resourceRequirements': [
                    {'type': 'VCPU', 'value': "4", },
                    {'type': 'MEMORY', 'value': "32768", }
                ],
                "environment": [
                    {"name": "SQL_QUERY", "value": sql_logic},
                    {"name": "memory", "value": "31GB"},
                    {"name": "AWS_DEFAULT_REGION", "value": "eu-central-1"},
                    {"name": "USUARIO_POSTGRES", "value": "neondb_owner"},
                    {"name": "CONTR_POSTGRES", "value": pg.password},
                    {"name": "HOST_POSTGRES", "value": pg.host},
                    {"name": "RUTA_S3_DUCKLAKE", "value": "s3://yena-s3-ducklake"}
                ]
            })

            source_query_half2 = f"""
                
                    WITH 
                        day_counts AS (
                        SELECT 
                            zone_type, 
                            cluster_id, 
                            pattern_name, 
                            COUNT(*) AS n_days
                        FROM gold.day_clusters
                        WHERE zone_type = '{zt}'
                        AND trip_date >= '2023-01-01 00:00:00' 
                        AND trip_date < '2023-12-31 23:59:59.999'
                        GROUP BY 1, 2, 3
                                ),

                    trip_aggregates AS (
                                SELECT 
                                    t.zone_type,
                                    dc.cluster_id,
                                    dc.pattern_name,
                                    EXTRACT(HOUR FROM t.date) AS hour_of_day,
                                    TRY_CAST(t.id_origin AS INTEGER) AS id_origin,
                                    TRY_CAST(t.id_destination AS INTEGER) AS id_destination,
                                    t.distance_group_km,
                                    
                                    SUM(t.n_trips) AS total_trips_period,
                                    
                                FROM silver.od_trips t

                                INNER JOIN gold.day_clusters dc 
                                    ON CAST(t.date AS DATE) = dc.trip_date 
                                    AND t.zone_type = dc.zone_type

                                WHERE t.zone_type = '{zt}'
                                AND t.date >= '2023-07-01 00:00:00' 
                                AND t.date < '2023-12-31 23:59:59.999'
                                
                                GROUP BY 1, 2, 3, 4, 5, 6, 7
                            )
                    
                    SELECT 
                        agg.zone_type,
                        agg.cluster_id,
                        agg.pattern_name,
                        agg.hour_of_day,
                        agg.id_origin,
                        agg.id_destination,
                        agg.distance_group_km,
                        ROUND(agg.total_trips_period / days.n_days, 2) AS avg_trips_per_day,
                        days.n_days AS n_days_in_cluster,
                        current_timestamp as ingestion_date
                    FROM trip_aggregates agg
                    INNER JOIN day_counts days 
                        ON agg.cluster_id = days.cluster_id 
                        AND agg.zone_type = days.zone_type
        
                        AND agg.pattern_name = days.pattern_name
                    """
            sql_logic2 = f"""

                MERGE INTO gold.typical_day_patterns2 AS target
                USING ({source_query_half2}) AS source
                ON target.zone_type = source.zone_type
                AND target.cluster_id = source.cluster_id
                AND target.hour_of_day = source.hour_of_day
                AND target.id_origin = source.id_origin
                AND target.id_destination = source.id_destination
                AND target.distance_group_km = source.distance_group_km
                WHEN MATCHED THEN
                UPDATE SET
                    avg_trips_per_day = target.avg_trips_per_day + source.avg_trips_per_day,
                WHEN NOT MATCHED THEN INSERT BY NAME;
                """.replace('\n', ' ').strip()

            batch_configs.append({
                'resourceRequirements': [
                    {'type': 'VCPU', 'value': "4", },
                    {'type': 'MEMORY', 'value': "32768", }
                ],
                "environment": [
                    {"name": "SQL_QUERY", "value": sql_logic2},
                    {"name": "memory", "value": "31GB"},
                    {"name": "AWS_DEFAULT_REGION", "value": "eu-central-1"},
                    {"name": "USUARIO_POSTGRES", "value": "neondb_owner"},
                    {"name": "CONTR_POSTGRES", "value": pg.password},
                    {"name": "HOST_POSTGRES", "value": pg.host},
                    {"name": "RUTA_S3_DUCKLAKE", "value": "s3://yena-s3-ducklake"}
                ]
            })
        return batch_configs

    @task
    def build_typical_day_table():
        #esto es solo para crear la tabla podemos conectarnos en local
        con = get_db_connection()
        con.sql(f"""
            CREATE TABLE IF NOT EXISTS gold.typical_day_patterns2 (
                zone_type VARCHAR,
                cluster_id INTEGER,
                pattern_name VARCHAR,
                hour_of_day INTEGER,
                id_origin INTEGER,
                id_destination INTEGER,
                distance_group_km VARCHAR,
                avg_trips_per_day DOUBLE,
                n_days_in_cluster INTEGER,
                ingestion_date TIMESTAMP);
    
     """)
        con.sql("ALTER TABLE gold.typical_day_patterns2 SET PARTITIONED BY (zone_type, pattern_name );")
        
    @task()
    def build_typical_day_sql(zones):
        
        batch_configs = []

        for zone in zones:
                # ESTO GUARDA SACA de staging y agrupa zona cluster hora origen, destino y grupo de distancia 
                source_query = f"""
                             
                                WITH zone_clusters_days AS(
                                SELECT zone_type, cluster_id, COUNT(*) AS n_days
                                    FROM gold.day_clusters
                                    WHERE zone_type = '{zone}' 
                                    GROUP BY 1,2
                                    ORDER BY zone_type, n_days DESC)
                                
                                SELECT 
                                g.zone_type,
                                g.cluster_id,
                                g.pattern_name,
                                g.hour_of_day,
                                TRY_CAST(g.id_origin AS INTEGER) AS id_origin,
                                TRY_CAST(g.id_destination AS INTEGER) AS id_destination ,
                                g.distance_group_km,
                                ROUND(SUM(sum_daily_trips) / c.n_days, 2) AS avg_trips_per_day,
                                
                                ROUND(SUM(sum_daily_trips),1) AS total_trips_in_cluster_sample,
                                c.n_days AS n_days_in_cluster,
                                
                                CURRENT_TIMESTAMP AS ingestion_date
                            FROM gold.staging_accumulated g 
                            LEFT JOIN zone_clusters_days c
                                ON g.zone_type = c.zone_type AND g.cluster_id = c.cluster_id
                            WHERE g.zone_type = '{zone}' 
                            GROUP BY 
                                g.zone_type, g.cluster_id, g.pattern_name, 
                                g.hour_of_day, id_origin,id_destination, g.distance_group_km, n_days_in_cluster
                            ORDER BY avg_trips_per_day DESC
                        """
                
                sql_logic = f"""

                MERGE INTO gold.typical_day_patterns AS target
                USING ({source_query}) AS source
                ON target.zone_type = source.zone_type
                AND target.cluster_id = source.cluster_id
                AND target.hour_of_day = source.hour_of_day
                AND target.id_origin = source.id_origin
                AND target.id_destination = source.id_destination
                AND target.distance_group_km = source.distance_group_km
                WHEN NOT MATCHED THEN INSERT BY NAME;
                """.replace('\n', ' ').strip()

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
    aux_table = aux_table_sql()
    day_patterns_tasks = BatchOperator.partial(
        task_id='gold-batch-patterns',
        job_name='gold-day-patterns',
        job_queue='duck_jobque',
        job_definition='duck_jobdef',
        region_name='eu-central-1',
        submit_job_timeout= 900,
        # Opcional: Aumentar timeout porque son cargas pesadas
        ).expand(container_overrides=aux_table)
    day_clusters = build_day_clusters(year=2023,n_clusters=3)
    
    

    #task_staging = create_staging()

    # batch_overrides = build_staging_accumulated_sql(zones=["gaus","municiples" ], years=[2023],months=[5])#list(range(3,13)) )
    # # Patrones GUARDA EN LA tabla staging por zona cluster hora origen, destino y grupo de distancia el numero de viajes para luego hacer el promedio de viajes
    # day_patterns_tasks = BatchOperator.partial(
    #     task_id='gold-batch-patterns',
    #     job_name='gold-day-patterns',
    #     job_queue='duck_jobque',
    #     job_definition='duck_jobdef',
    #     region_name='eu-central-1',
    #     submit_job_timeout= 900,
    #     # Opcional: Aumentar timeout porque son cargas pesadas
        
    # ).expand(container_overrides=batch_overrides)

    # # batch_overrides_dist = build_staging_accumulated_sql_dist(zone="districts", years=[2023],months=list(range(3,13)) )

    # # #DIFERENTE PARA DISTRITOS NECESITA QUERYS MAS PEQUEÑAS separa el mes en 4 partes 
    # # day_patterns_tasks_dist = BatchOperator.partial(
    # #     task_id='gold-batch-patterns_districts',
    # #     job_name='gold-day-patterns_districts',
    # #     job_queue='duck_jobque',
    # #     job_definition='duck_jobdef',
    # #     region_name='eu-central-1',
    # #     submit_job_timeout= 1200,
    # #     # Opcional: Aumentar timeout porque son cargas pesadas 
    # #).expand(container_overrides=batch_overrides_dist)
    
    # typical_day_table = build_typical_day_table()
    # typical_day_overrides = build_typical_day_sql(zones=["gaus","municiples","districts"])

    # day_patterns = BatchOperator.partial(
    #     task_id='gold-day-patterns-final',
    #     job_name='gold-day-patterns-final',
    #     job_queue='duck_jobque',
    #     job_definition='duck_jobdef',
    #     region_name='eu-central-1',
    #     submit_job_timeout= 1200,
    #     # Opcional: Aumentar timeout porque son cargas pesadas
        
    # ).expand(container_overrides=typical_day_overrides)

    typical_day_table = build_typical_day_table()
    typical_day_table_sql = typical_day_patterns_sql(zones=["gaus","municiples","districts"])

    
    day_patterns = BatchOperator.partial(
        task_id='gold-day-patterns-final',
        job_name='gold-day-patterns-final',
        job_queue='duck_jobque',
        job_definition='duck_jobdef',
        region_name='eu-central-1',
        submit_job_timeout= 1200,
        # Opcional: Aumentar timeout porque son cargas pesadas
        
    ).expand(container_overrides=typical_day_table_sql)
    gold_init >> day_clusters 
    day_clusters >> day_patterns_tasks
    day_patterns_tasks >> typical_day_table # CAMBIAR FINAL day_clusters >> task_staging
    typical_day_table >> typical_day_table_sql
    # task_staging >> batch_overrides
    # day_patterns_tasks >> batch_overrides_dist

    # day_patterns_tasks >> typical_day_table
    # typical_day_table >> typical_day_overrides


gold1 = business1_dag()
