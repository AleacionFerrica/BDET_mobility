from airflow.sdk import Asset, dag, task
from airflow.sdk.bases.hook import BaseHook
from airflow.providers.amazon.aws.operators.batch import BatchOperator
import duckdb
import pandas as pd
import numpy as np
import requests
import json
from datetime import datetime, timedelta
import os
from pathlib import Path
import xml.etree.ElementTree as ET
import calendar
import re
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

pg = BaseHook.get_connection("neon_postgres")
aws = BaseHook.get_connection("aws_default")
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/usr/local/airflow")
DB_PATH = "include/mobility.ducklake"

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
    dag_id='gold_b1',
    #schedule_interval='@weekly', # Ejecutar semanalmente o cuando quieras
    #start_date=days_ago(1),
    default_args={'owner': 'airflow','retries': 3,'retry_delay': timedelta(minutes=1)},

    
    catchup=False,
    max_active_tasks=1,
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

    #CLUSTERS: k-means
    @task
    def build_day_clusters(
        year: int,
        n_clusters: int = 3,
        include_volume_in_clustering: bool = True,
        random_state: int = 42) -> None:


        zone_types = ["districts", "municiples", "gaus"]

        if not zone_types:
            print(f"[BQ1] No zone_types found for year={year}.")
            return

        zone_list_sql = ", ".join([f"'{z}'" for z in zone_types])
        con = get_db_connection()
        for zt in zone_types:
            df_temporal = con.sql(
                f"""
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
                ORDER BY 1,2,3
                """
            ).df()
            #print(df_temporal)
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


   
        con.sql("CREATE TABLE IF NOT EXISTS gold.day_clusters AS SELECT *,current_timestamp as ingestion_date FROM df_clusters LIMIT 0;")

        con.sql(
            """
            BEGIN TRANSACTION;
            MERGE INTO  gold.day_clusters as target
            USING (SELECT *, current_timestamp as ingestion_date
                    FROM df_clusters) AS sc
                    ON target.ine_section = sc.ine_section
                    AND target.concept = sc.concept
                    AND sc.name = sc.name
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

    # def build_typical_day_patterns(
        
    #     year: int = 2023,
    #     zone_types: list[str] | None = None,) :


    #     if not zone_types:
    #         print(f"[BQ1] No zone_types found for year={year}.")
    #         return
    #     con = get_db_connection()
    #     zone_list_sql = ", ".join([f"'{z}'" for z in zone_types])
    #     daily_od_hour_q = f"""
    #         SELECT
    #             date AS trip_date,
    #             t.zone_type,
    #             EXTRACT(HOUR FROM t.date) AS hour_of_day,

    #             t.id_origin,
    #             t.id_destination,
    #             t.distance_group_km,
    #             SUM(t.n_trips) AS daily_trips,
    #             SUM(t.trips_total_length_km) AS daily_total_length_km,
                
    #             SUM(t.trips_total_length_km) / NULLIF(SUM(t.n_trips), 0) AS daily_avg_trip_length_km
    #         FROM silver.od_trips t
    #         WHERE t.zone_type = 'gaus'
    #         AND date >= '{date_str} 00:00:00' 
    #         AND date < '{date_end} 23:59:59'
    #         GROUP BY 1,2,3,4,5,6"""
        
    #     con.sql(""" 
    #             CREATE TABLE IF NOT EXISTS  gold.daily_od_hour (
    #                 trip_date TIMESTAMP,
    #                 zone_type VARCHAR,
    #                 hour_of_day INTEGER,
    #                 id_origin VARCHAR,
    #                 id_destination VARCHAR,
    #                 distance_group_km VARCHAR,
    #                 n_trips DOUBLE,
    #                 trips_total_length_km DOUBLE,

    #                 origin_activity_std boolean,
    #                 destination_activity_std boolean,

    #                 ingestion_date TIMESTAMP
    #             );
    #     """)
    #     for zone in zone_types:
    #         for month in range(1,13) :
    #             year = 2023
    #             month = 3
    #             date_str = f"{year}-{month:02d}-{1:02d}"
    #             date_end = f"{year}-{month:02d}-{31:02d}"
    #             daily_od_hour_q = f"""
    #             SELECT
    #                 date AS trip_date,
    #                 t.zone_type,
    #                 EXTRACT(HOUR FROM t.date) AS hour_of_day,

    #                 t.id_origin,
    #                 t.id_destination,
    #                 t.distance_group_km,
    #                 SUM(t.n_trips) AS daily_trips,
    #                 SUM(t.trips_total_length_km) AS daily_total_length_km,
                    
    #                 SUM(t.trips_total_length_km) / NULLIF(SUM(t.n_trips), 0) AS daily_avg_trip_length_km
    #             FROM silver.od_trips t
    #             WHERE t.zone_type = '{zone}'
    #             AND date >= '{date_str} 00:00:00' 
    #             AND date < '{date_end} 23:59:59'
    #             GROUP BY 1,2,3,4,5,6"""

    #             con.sql(
    #                 f""" 
    #                 MERGE INTO
                    
    #                 """
    #             )

    #     con.sql("DELETE FROM _daily_od_hour WHERE id_origin IS NULL OR id_destination IS NULL;")

    #     con.sql(
    #         """
    #         CREATE OR REPLACE TABLE gold.typical_day_patterns AS
    #         SELECT
    #             dc.zone_type,
    #             dc.cluster_id,
    #             dc.pattern_name,
    #             d.hour_of_day,
    #             d.id_origin,
    #             d.id_destination,

    #             ROUND(AVG(d.daily_trips),2) AS avg_trips_per_day,
    #             ROUND(AVG(d.daily_avg_trip_length_km),2) AS avg_trip_length_km,
    #             d.distance_group_km as distance_group_km,
    #             ROUND(SUM(d.daily_trips),2) AS total_trips_in_cluster_sample,
    #             COUNT(DISTINCT d.trip_date) AS n_days_in_cluster,

    #             CURRENT_TIMESTAMP AS ingestion_date
    #         FROM gold.daily_od_hour d
    #         JOIN gold.day_clusters dc
    #         ON d.trip_date = dc.trip_date
    #         AND d.zone_type = dc.zone_type
    #         GROUP BY
    #             dc.zone_type, dc.cluster_id, dc.pattern_name,
    #             d.hour_of_day, d.id_origin, d.id_destination, distance_group_km
    #         """
    #     )

    #     print("[BQ1] gold.typical_day_patterns created. Rows per pattern:")
    #     con.sql(
    #         """
    #         SELECT zone_type, pattern_name, COUNT(*) AS rows
    #         FROM gold.typical_day_patterns
    #         GROUP BY 1,2
    #         ORDER BY zone_type, rows DESC
    #         """
    #     ).show()
    
    gold_init = gold_schema()
    day_clusters = build_day_clusters(year=2023)

    gold_init >> day_clusters

gold1 = business1_dag()