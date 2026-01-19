from airflow.sdk import Asset, dag, task, Param, get_current_context
from airflow.sdk.bases.hook import BaseHook
from airflow.providers.amazon.aws.operators.batch import BatchOperator
import duckdb
import pandas as pd
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
import seaborn as sns 
from shapely import wkt, wkb
from shapely.geometry import LineString
import geopandas as gpd
import matplotlib.colors as mcolors
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
#geometry = "POLYGON ((736836.694 4519821.713, 736249.259 4518242.978, 735726.778 4520075.176, 799491.086 4492725.872, 782537.968 4291729.411, 697544.085 4190052.463, 675786.982 4212163.570, 671639.955 4248496.830, 669387.960 4264213.129, 680061.177 4279703.821, 681256.136 4311110.071, 666970.379 4310795.232, 650105.486 4320887.080, 660567.343 4352953.120, 650093.189 4355575.209, 633401.509 4362343.305, 627949.637 4377316.613, 646520.518 4394092.025, 634956.375 4435073.582, 645482.108 4448329.542, 684127.010 4417872.078, 720452.304 4461794.220, 736836.694 4519821.713))"
matplotlib.use('Agg')
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
    dag_id='Test_final',
    #schedule_interval='@weekly', # Ejecutar semanalmente o cuando quieras
    #start_date=days_ago(1),
    default_args={'owner': 'airflow','retries': 3,'retry_delay': timedelta(minutes=1)},

    
    catchup=False,
    max_active_tasks=10,
    params={
        "start_date": Param(
            default="2023-01-01", 
            type="string", 
            format="date", 
            title="Start Date",
            description="Analysis start date (YYYY-MM-DD)."
        ),
        "end_date": Param(
            default="2023-01-31", 
            type="string", 
            format="date", 
            title="End Date", 
            description="Analysis end date (YYYY-MM-DD)."
        ),
        "target_geometry": Param(
            default=None, 
            type=["string", "null"], 
            title="Region Geometry (WKT/GeoJSON)",
            description="Optional: Enter a WKT Polygon (e.g., 'POLYGON((...))') to filter specific areas. Leave empty for full Spain."
        )
    },
    tags=['master', 'duckdb', 'test'] )

def bq1_dag():
    @task
    def gold_schema() -> None:
        ctx = get_current_context()
        print(ctx["params"]["start_date"])
        con = get_db_connection()
        con.sql("CREATE SCHEMA IF NOT EXISTS test;")
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

        ctx = get_current_context()
        start_date = ctx["params"]["start_date"]
        end_date = ctx["params"]["end_date"]
        try:
            s_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
            e_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
            
            # Formato deseado: "DD-MM" (Ej: 22-03)
            start_short = s_date_obj.strftime('%d-%m')
            end_short = e_date_obj.strftime('%d-%m')
        except Exception as e:
            # Por si acaso el formato no era YYYY-MM-DD, usamos el string original
            print(f"Aviso: No se pudo formatear la fecha: {e}")
            start_short = start_date
            end_short = end_date
        zone_types = ["gaus","municiples","districts" ]
        table_sufix = f"_{start_short}_{end_short}".replace('-', '_')
        if not zone_types:
            print(f"[BQ1] No zone_types found for year={year}.")
            return

        zone_list_sql = ", ".join([f"'{z}'" for z in zone_types])
        
        for zt in zone_types:
            con = get_db_connection()
            df_temporal = con.sql(
                f"""
                SELECT
                    CAST(date AS DATE) AS trip_date,
                    zone_type,
                    EXTRACT(HOUR FROM date) AS hour_of_day,
                    SUM(n_trips) AS total_trips
                FROM silver.od_trips
                WHERE zone_type = '{zt}'
                AND date >= '{start_date} 00:00:00' 
                AND date < '{end_date} 23:59:59.999'
                GROUP BY 1,2,3
                ORDER BY 1,2,3
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


            #con.sql("DROP TABLE test.day_clusters ")
            con.sql(f"CREATE TABLE IF NOT EXISTS test.day_clusters{table_sufix} AS SELECT *,current_timestamp as ingestion_date FROM df_clusters LIMIT 0 ;")

            con.sql(
                f"""
                BEGIN TRANSACTION;
                MERGE INTO  test.day_clusters{table_sufix} as target
                USING (SELECT *, current_timestamp as ingestion_date

                        FROM df_clusters) AS sc
                        ON target.trip_date = sc.trip_date
                        AND target.zone_type = sc.zone_type
                        WHEN NOT MATCHED THEN
                            INSERT BY NAME;
                COMMIT;
                """)

        print("[BQ1] test.day_clusters created. Days per cluster:")
        con.sql(
            f"""
            SELECT zone_type, cluster_id, pattern_name, COUNT(*) AS n_days
            FROM test.day_clusters{table_sufix}
            WHERE trip_date >= '{start_date} 00:00:00' 
            AND trip_date < '{end_date} 23:59:59.999'
            GROUP BY 1,2,3
            ORDER BY zone_type, n_days DESC
            """
        ).show()
        #return df_clusters
   

    @task()
    def build_typical_pattern_sql():
        ctx = get_current_context()
        start_date = ctx["params"]["start_date"]
        end_date = ctx["params"]["end_date"]
        geometry = wkt.loads(ctx["params"]["target_geometry"]).wkb_hex
        batch_configs = []
        try:
            s_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
            e_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
            
            # Formato deseado: "DD-MM" (Ej: 22-03)
            start_short = s_date_obj.strftime('%d-%m')
            end_short = e_date_obj.strftime('%d-%m')
        except Exception as e:
            # Por si acaso el formato no era YYYY-MM-DD, usamos el string original
            print(f"Aviso: No se pudo formatear la fecha: {e}")
            start_short = start_date
            end_short = end_date
        table_sufix = f"_{start_short}_{end_short}".replace('-', '_')
        source_query1 = f"""
        WITH 
            day_counts AS (
            SELECT 
                zone_type, 
                cluster_id, 
                pattern_name, 
                COUNT(*) AS n_days
            FROM test.day_clusters{table_sufix}
            WHERE trip_date >= '{start_date} 00:00:00' 
            AND trip_date < '{end_date} 23:59:59.999'
            GROUP BY 1, 2, 3
                    ),
        target_zones AS ( 
            SELECT 
                id_zone,
                zone_type 
            FROM silver.zones_info
            WHERE CASE
                WHEN unhex('{geometry}') IS NOT NULL AND '{geometry}' != '' THEN
                    ST_Intersects(
                    
                    ST_Transform(ST_GeomFromWKB(unhex('{geometry}')), 'EPSG:4326', 'EPSG:25830'), 
                    
                    geometry)
                ELSE TRUE
            END
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
                        SUM(t.trips_total_length_km) AS total_length_period
                        
                    FROM silver.od_trips t

                    INNER JOIN test.day_clusters{table_sufix} dc 
                        ON CAST(t.date AS DATE) = dc.trip_date 
                        AND t.zone_type = dc.zone_type

                    INNER JOIN target_zones zo
                        ON t.id_origin = zo.id_zone 
                        AND t.zone_type = zo.zone_type
                        

                    INNER JOIN target_zones zd
                        ON t.id_destination = zd.id_zone 
                        AND t.zone_type = zd.zone_type

                    WHERE t.date >= '{start_date} 00:00:00' 
                    AND t.date < '{end_date} 23:59:59.999'
                    
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
            ROUND(agg.total_length_period / days.n_days, 2) AS avg_length_km_per_day,
            
            days.n_days AS days_count_basis

        FROM trip_aggregates agg
        INNER JOIN day_counts days 
            ON agg.cluster_id = days.cluster_id 
            AND agg.zone_type = days.zone_type

            AND agg.pattern_name = days.pattern_name
        """
        sql_logic1 = f"""
        BEGIN TRANSACTION;
        CREATE TABLE IF NOT EXISTS test.typical_day_pattern{table_sufix} AS ({source_query1});
        COMMIT;
        """.replace('\n', ' ').strip()

        batch_configs.append({
        'resourceRequirements': [
            {'type': 'VCPU', 'value': "2", },
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
    def hourly_plots(zone):
        ctx = get_current_context()
        start_date = ctx["params"]["start_date"]
        end_date = ctx["params"]["end_date"]
        
        
        sns.set_theme(style="whitegrid")
        try:
            s_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
            e_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
            
            # Formato deseado: "DD-MM" (Ej: 22-03)
            start_short = s_date_obj.strftime('%d-%m')
            end_short = e_date_obj.strftime('%d-%m')
        except Exception as e:
            # Por si acaso el formato no era YYYY-MM-DD, usamos el string original
            print(f"Aviso: No se pudo formatear la fecha: {e}")
            start_short = start_date
            end_short = end_date
        table_sufix = f"_{start_short}_{end_short}".replace('-', '_')
        con = get_db_connection()
        

        for zt in zone:
            query = f"""
                SELECT 
                    pattern_name, 
                    hour_of_day, 
                    SUM(avg_trips_per_day) as avg_trips_per_day
                FROM test.typical_day_pattern{table_sufix}
                WHERE zone_type = '{zt}'
                GROUP BY pattern_name, hour_of_day
                ORDER BY pattern_name, hour_of_day
            """
            df_zt= con.sql(query).df()
            
            # Conversiones
            df_zt['avg_trips_per_day'] = pd.to_numeric(df_zt['avg_trips_per_day'])
            df_zt['hour_of_day'] = pd.to_numeric(df_zt['hour_of_day'])

            # Agrupar
            hourly_data = df_zt.groupby(['pattern_name', 'hour_of_day'])['avg_trips_per_day'].sum().reset_index()

            # --- Generación del Gráfico ---
            plt.figure(figsize=(14, 7))
            
            
            sns.lineplot(
                data=hourly_data, 
                x='hour_of_day', 
                y='avg_trips_per_day', 
                hue='pattern_name', 
                marker='o', 
                linewidth=2
            )

            plt.title(f'Average Hourly Trips - {zt} ({start_date} / {end_date})', fontsize=16)
            plt.xlabel('Hour of Day (0-23)', fontsize=12)
            plt.ylabel('Average Daily Trips', fontsize=12)
            plt.xticks(range(0, 24))
            plt.grid(True, which='both', linestyle='--', alpha=0.7)

            output_dir = os.getcwd() + "/dags/output_plots/hourly"
            
            # Nombre del archivo
            filename = f"{output_dir}/{zt}_{start_short}-{end_short}.png"
            
            # Guardar
            print(f"Guardando gráfico en: {filename}")
            plt.savefig(filename, bbox_inches='tight')
            plt.close() 

    @task
    def map_plots(zone):
        ctx = get_current_context()
        start_date = ctx["params"]["start_date"]
        end_date = ctx["params"]["end_date"]
        geometry = wkt.loads(ctx["params"]["target_geometry"]).wkb_hex
        
        sns.set_theme(style="whitegrid")

        try:
            s_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
            e_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
            
            # Formato deseado: "DD-MM" (Ej: 22-03)
            start_short = s_date_obj.strftime('%d-%m')
            end_short = e_date_obj.strftime('%d-%m')
        except Exception as e:
            # Por si acaso el formato no era YYYY-MM-DD, usamos el string original
            print(f"Aviso: No se pudo formatear la fecha: {e}")
            start_short = start_date
            end_short = end_date
        table_sufix = f"_{start_short}_{end_short}".replace('-', '_')
        con = get_db_connection()
        

        for zt in zone:
            query = f"""--sql
                WITH 
                    -- 1. DEFINICIÓN DE ZONAS DE ESTUDIO (Tu filtro maestro)
                    target_zones AS ( 
                        SELECT 
                            id_zone,
                            name_zone,
                            zone_type,
                            geometry,
                            centroid,
                            visual_point
                        FROM silver.zones_info
                        WHERE CASE
                            WHEN unhex('{geometry}') IS NOT NULL AND unhex('{geometry}') != '' THEN
                                ST_Intersects(ST_Transform(ST_GeomFromWKB(unhex('{geometry}')), 'EPSG:4326', 'EPSG:25830'), geometry)
                            ELSE TRUE
                        END
                    ),

                    -- 2. Clústers
                    global_days AS (
                        SELECT cluster_id, pattern_name, COUNT(*) AS n_days
                        FROM test.day_clusters{table_sufix}
                        WHERE zone_type = '{zt}'
                        GROUP BY 1,2
                        ORDER BY n_days DESC
                    ),

                    -- 3. CÁLCULO DE ORÍGENES (Salidas)
                    origins AS (
                        SELECT 
                            zone_type,
                            cluster_id,
                            id_origin AS id_zone,
                            SUM(CASE WHEN hour_of_day IN (6,7,8,9) THEN avg_trips_per_day ELSE 0 END) AS trips_out_morning,
                            SUM(CASE WHEN hour_of_day IN (17,18,19) THEN avg_trips_per_day ELSE 0 END) AS trips_out_afternoon
                        FROM test.typical_day_pattern{table_sufix}
                        WHERE id_origin <> id_destination
                        AND zone_type = '{zt}'
                        AND hour_of_day IN (6, 7, 8, 9, 17, 18, 19)
                        GROUP BY 1, 2, 3
                    ),

                    -- 4. CÁLCULO DE DESTINOS (Entradas)
                    destinations AS (
                        SELECT 
                            zone_type,
                            cluster_id,
                            id_destination AS id_zone,
                            SUM(CASE WHEN hour_of_day IN (6,7,8,9) THEN avg_trips_per_day ELSE 0 END) AS trips_in_morning,
                            SUM(CASE WHEN hour_of_day IN (17,18,19) THEN avg_trips_per_day ELSE 0 END) AS trips_in_afternoon
                        FROM test.typical_day_pattern{table_sufix}
                        WHERE id_origin <> id_destination
                        AND zone_type = '{zt}' 
                        AND hour_of_day IN (6, 7, 8, 9, 17, 18, 19)
                        GROUP BY 1, 2, 3
                    )

                SELECT 
                    tz.zone_type,       -- Usamos el tipo de la tabla target directamente
                    g.pattern_name,
                    tz.id_zone,         -- Usamos el ID de la tabla target directamente
                    
                    -- Info de la zona (Garantizado que existe porque es un INNER JOIN)
                    tz.name_zone,
                    ST_AsWKB(tz.geometry) as geometry,
                    tz.centroid,
                    tz.visual_point,
                    
                    -- Métricas (Si no hay datos de viaje, ponemos 0)
                    ROUND(COALESCE(o.trips_out_morning, 0), 2) as origin_morning,
                    ROUND(COALESCE(o.trips_out_afternoon, 0), 2) as origin_afternoon,
                    
                    ROUND(COALESCE(d.trips_in_morning, 0), 2) as destination_morning,
                    ROUND(COALESCE(d.trips_in_afternoon, 0), 2) as destination_afternoon

                FROM target_zones tz -- <--- CAMBIO CLAVE: Empezamos desde tu lista maestra
                INNER JOIN global_days g 
                    ON 1=1 -- Producto cartesiano controlado (queremos las zonas para cada pattern)

                -- Unimos los datos de Origen a tus zonas objetivo
                LEFT JOIN origins o
                    ON tz.id_zone = o.id_zone 
                    AND tz.zone_type = o.zone_type
                    AND g.cluster_id = o.cluster_id

                -- Unimos los datos de Destino a tus zonas objetivo
                LEFT JOIN destinations d 
                    ON tz.id_zone = d.id_zone 
                    AND tz.zone_type = d.zone_type
                    AND g.cluster_id = d.cluster_id

                -- Filtro final de limpieza por si acaso
                WHERE (o.id_zone IS NOT NULL OR d.id_zone IS NOT NULL)"""

            df_zt= con.sql(query).df()
            df_weekday = df_zt[df_zt['pattern_name'] == 'Weekday']
            df_weekday['geometry'] = df_weekday['geometry'].apply(lambda x: wkb.loads(bytes(x)))
            gdf = gpd.GeoDataFrame(df_weekday, geometry='geometry')

            gdf['total_volume_morning'] = gdf['origin_morning'] + gdf['destination_morning']
            gdf['balance_morning'] = np.where(
            gdf['total_volume_morning'] > 0,( (gdf['destination_morning'] - gdf['origin_morning'])  / gdf['total_volume_morning']),
            0 # Si no hay tráfico, asumimos equilibrio (o puedes poner np.nan)
            )
            print("min: ",min(gdf["balance_morning"]))
            print("max: ",max(gdf["balance_morning"]))

            gdf['total_volume_afternoon'] = gdf['origin_afternoon'] + gdf['destination_afternoon']
            gdf['balance_afternoon'] = np.where(
            gdf['total_volume_afternoon'] > 0, ((gdf['destination_afternoon'] - gdf['origin_afternoon'])  / gdf['total_volume_afternoon']),
            0 # Si no hay tráfico, asumimos equilibrio (o puedes poner np.nan)
            )
            print("min: ",min(gdf["balance_afternoon"]))
            print("max: ",max(gdf["balance_afternoon"]))

            # MAÑANA
            vmin_m = gdf['balance_morning'].min()
            vmax_m = gdf['balance_morning'].max()

            if vmin_m >= 0: vmin_m = -0.1
            if vmax_m <= 0: vmax_m = 0.1
            div_morn = mcolors.TwoSlopeNorm(vmin=vmin_m, vcenter=0, vmax=vmax_m)

            vmin_a =  gdf['balance_afternoon'].min()
            vmax_a = gdf['balance_afternoon'].max()

            # Ajuste defensivo por si todos son positivos o negativos
            if vmin_a >= 0: vmin_a = -0.1
            if vmax_a <= 0: vmax_a = 0.1

            
            div_afte = mcolors.TwoSlopeNorm(vmin=vmin_a, vcenter=0, vmax=vmax_a)
            fig, axes = plt.subplots(1, 2, figsize=(24, 12))
            plt.suptitle(f"Mobility Balance Comparison: {zt} ({start_short} / {end_short})", fontsize=24)

            gdf.plot(
                column='balance_morning',
                cmap='RdBu', 
                norm=div_morn,
                linewidth=0.5,
                edgecolor='grey',
                legend=True,
                legend_kwds={'label': "Balance Index (+1 Pull | -1 Source)", 'shrink': 0.6},
                ax=axes[0],
                missing_kwds={"color": "lightgrey", "hatch": "///"}
                )
            axes[0].set_title(f"Morning Balance (06-09h)", fontsize=18)
            axes[0].ticklabel_format(useOffset=False, style='plain')
            axes[0].set_xlabel("Longitude")
            axes[0].set_ylabel("Latitude")
            gdf.plot(
                column='balance_afternoon',
                cmap='RdBu', 
                norm=div_afte,
                linewidth=0.5,
                edgecolor='grey',
                legend=True,
                legend_kwds={'label': "Balance Index (+1 Pull | -1 Source)", 'shrink': 0.6},
                ax=axes[1],
                missing_kwds={"color": "lightgrey", "hatch": "///"}
            )
            axes[1].set_title(f"Afternoon Balance (17-20h)", fontsize=18)
            axes[1].ticklabel_format(useOffset=False, style='plain')
            axes[1].set_xlabel("Longitude")
            axes[1].set_ylabel("Latitude")    




            plt.tight_layout()
            output_dir = os.getcwd() + "/dags/output_plots/travel_balance"
            
            # Nombre del archivo
            filename = f"{output_dir}/{zt}_Travel_Balance_{start_short}-{end_short}.png"
            
            # Guardar
            #print(f"Guardando gráfico en: {filename}")
            plt.savefig(filename, bbox_inches='tight')
            plt.close() 
        
        
    # -----------------BQ2-------------------------------------
    @task
    def sql_gravity_pair( zones):
        ctx = get_current_context()
        start_date = ctx["params"]["start_date"]
        end_date = ctx["params"]["end_date"]
        geometry = wkt.loads(ctx["params"]["target_geometry"]).wkb_hex
        
        sns.set_theme(style="whitegrid")
        try:
            s_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
            e_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
            
            # Formato deseado: "DD-MM" (Ej: 22-03)
            start_short = s_date_obj.strftime('%d-%m')
            end_short = e_date_obj.strftime('%d-%m')
        except Exception as e:
            # Por si acaso el formato no era YYYY-MM-DD, usamos el string original
            print(f"Aviso: No se pudo formatear la fecha: {e}")
            start_short = start_date
            end_short = end_date
        table_sufix = f"_{start_short}_{end_short}".replace('-', '_')
        con = get_db_connection()
        batch_configs = [ ]
        for zt in zones:
            query = f"""
                WITH
                target_zones AS ( 
                    SELECT 
                        id_zone,
                        zone_type 
                    FROM silver.zones_info
                    WHERE CASE
                        WHEN unhex('{geometry}') IS NOT NULL AND '{geometry}' != '' THEN
                            ST_Intersects(ST_Transform(ST_GeomFromWKB(unhex('{geometry}')), 'EPSG:4326', 'EPSG:25830'),  geometry)
                        ELSE TRUE
                    END
                ),
                od AS (SELECT 
                        TRY_CAST(id_origin AS INTEGER) AS id_origin,
                        TRY_CAST(id_destination AS INTEGER) AS id_destination,
                        SUM(n_trips) AS actual_trips
                    FROM silver.od_trips t
                    INNER JOIN target_zones zo
                        ON t.id_origin = zo.id_zone 
                        AND t.zone_type = zo.zone_type
                    INNER JOIN target_zones zd
                        ON t.id_destination = zd.id_zone 
                        AND t.zone_type = zd.zone_type
                    WHERE t.zone_type = '{zt}'
                    AND t.date >= '{start_date} 00:00:00' 
                    AND t.date < '{end_date} 23:59:59.999'
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
                        AVG(income) AS inc
                    FROM silver.average_income i
                    LEFT JOIN aux_inc a 
                    ON i.id_zone = a.id_districts_ine 
                    WHERE i.year = 2023
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
                    WHERE year = 2023
                    AND id_{zt}_mitma NOT NULL
                    GROUP BY 1
                    ),
                
            joined AS(
                SELECT
                        '{zt}' AS zone_type,
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
            sql_logic= f"""
                        CREATE TABLE IF NOT EXISTS test.gravity_pair_features{table_sufix} AS ({query}) LIMIT 0;
                           
                            """.replace('\n', ' ').strip()

            con.sql(sql_logic)
            sql_logic1 = f"""
                       
                            MERGE INTO test.gravity_pair_features{table_sufix} as target
                            USING ({query} )AS source
                                ON target.zone_type = source.zone_type 
                                AND target.id_origin = source.id_origin
                                AND target.id_destination = source.id_destination
                                AND target.distance_km = source.distance_km
                                WHEN NOT MATCHED THEN
                                    INSERT BY NAME;
                            """.replace('\n', ' ').strip()

            batch_configs.append({
                'resourceRequirements': [
                    {'type': 'VCPU', 'value': "2", },
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
    def fit_gravity_k():
        ctx = get_current_context()
        start_date = ctx["params"]["start_date"]
        end_date = ctx["params"]["end_date"]
        geometry = wkt.loads(ctx["params"]["target_geometry"]).wkb_hex
        
        sns.set_theme(style="whitegrid")
        try:
            s_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
            e_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
            
            # Formato deseado: "DD-MM" (Ej: 22-03)
            start_short = s_date_obj.strftime('%d-%m')
            end_short = e_date_obj.strftime('%d-%m')
        except Exception as e:
            # Por si acaso el formato no era YYYY-MM-DD, usamos el string original
            print(f"Aviso: No se pudo formatear la fecha: {e}")
            start_short = start_date
            end_short = end_date
        table_sufix = f"_{start_short}_{end_short}".replace('-', '_')
        con = get_db_connection()
        #con.sql("DROP TABLE test.gravity_params")
        con.sql("""
            CREATE TABLE IF NOT EXISTS test.gravity_params (
                time_range VARCHAR,
                zone_type VARCHAR,
                k DOUBLE,
                n_pairs_used BIGINT,
                fitted_at TIMESTAMP
            );
        """)
        #con.sql(f"DELETE FROM gold.gravity_params WHERE zone_type='{zt}' AND year={year};")
        
        query= f"""
        WITH bounds AS (
            SELECT 
                zone_type,
                quantile_cont(x_ij, 0.25) as lower_limit,
                quantile_cont(x_ij, 0.75) as upper_limit  
            FROM test.gravity_pair_features{table_sufix} 
            WHERE x_ij IS NOT NULL AND x_ij > 0
            AND actual_trips IS NOT NULL
            GROUP BY 1,
        )

        SELECT
            '{table_sufix}' as time_range,
            t.zone_type AS zone_type,

            SUM(t.x_ij * t.actual_trips) / NULLIF(SUM(t.x_ij * t.x_ij), 0) AS k,
            COUNT(*) AS n_pairs_used,
            CURRENT_TIMESTAMP AS fitted_at
        FROM test.gravity_pair_features{table_sufix}  t, bounds b
        WHERE t.zone_type=b.zone_type
            AND t.x_ij IS NOT NULL 
            AND t.actual_trips IS NOT NULL

            AND t.x_ij >= b.lower_limit 
            AND t.x_ij <= b.upper_limit
        GROUP BY t.zone_type
            """
        
        sql_logic2= f"""
                        MERGE INTO test.gravity_params as target
                        USING ({query} )AS source
                            ON target.time_range = source.time_range 
                            AND target.zone_type = source.zone_type 
                            AND target.n_pairs_used = source.n_pairs_used
                            WHEN NOT MATCHED THEN
                                INSERT BY NAME;
                        """
        con.sql(sql_logic2)

    @task
    def infrastructure_gap_sql():
        ctx = get_current_context()
        start_date = ctx["params"]["start_date"]
        end_date = ctx["params"]["end_date"]
        geometry = wkt.loads(ctx["params"]["target_geometry"]).wkb_hex
        
        sns.set_theme(style="whitegrid")
        try:
            s_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
            e_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
            
            # Formato deseado: "DD-MM" (Ej: 22-03)
            start_short = s_date_obj.strftime('%d-%m')
            end_short = e_date_obj.strftime('%d-%m')
        except Exception as e:
            # Por si acaso el formato no era YYYY-MM-DD, usamos el string original
            print(f"Aviso: No se pudo formatear la fecha: {e}")
            start_short = start_date
            end_short = end_date
        table_sufix = f"_{start_short}_{end_short}".replace('-', '_')
        batch_configs = []
        con = get_db_connection()
        
        query = f"""WITH kpar AS (
            SELECT zone_type, k
            FROM test.gravity_params
            WHERE time_range='{table_sufix}'
        )
        SELECT
            f.zone_type,
            f.id_origin,
            f.id_destination,
            f.distance_km,
            f.actual_trips,
            (k.k * f.x_ij) AS theoretical_trips,
            f.actual_trips / NULLIF((k.k * f.x_ij), 0) AS mismatch_ratio,
            GREATEST((k.k * f.x_ij) - f.actual_trips, 0) AS gap,
            CURRENT_TIMESTAMP AS ingestion_date
        FROM test.gravity_pair_features{table_sufix} f
        INNER JOIN kpar k
            ON f.zone_type = k.zone_type
        WHERE  f.x_ij IS NOT NULL AND f.x_ij > 0"""
        sql_logic = f"""
        BEGIN TRANSACTION;
        CREATE TABLE IF NOT EXISTS test.infrastructure_gaps{table_sufix} AS ({query});
        COMMIT;
        """
        
        con.sql(sql_logic)

    @task
    def gap_plots(zones):
        ctx = get_current_context()
        start_date = ctx["params"]["start_date"]
        end_date = ctx["params"]["end_date"]
        geometry = wkt.loads(ctx["params"]["target_geometry"]).wkb_hex
        colores = ['red', 'purple', 'blue']
        sns.set_theme(style="whitegrid")
        try:
            s_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
            e_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
            
            # Formato deseado: "DD-MM" (Ej: 22-03)
            start_short = s_date_obj.strftime('%d-%m')
            end_short = e_date_obj.strftime('%d-%m')
        except Exception as e:
            # Por si acaso el formato no era YYYY-MM-DD, usamos el string original
            print(f"Aviso: No se pudo formatear la fecha: {e}")
            start_short = start_date
            end_short = end_date
        table_sufix = f"_{start_short}_{end_short}".replace('-', '_')
        batch_configs = []
        con = get_db_connection()
        df_geo = con.sql(f""" 
                        SELECT 
                            id_zone,
                            name_zone,
                            zone_type,
                            ST_AsWKB(geometry)as geometry,

                        FROM silver.zones_info
                        WHERE CASE
                            WHEN unhex('{geometry}') IS NOT NULL AND unhex('{geometry}') != '' THEN
                                ST_Intersects(ST_Transform(ST_GeomFromWKB(unhex('{geometry}')), 'EPSG:4326', 'EPSG:25830'),  geometry)
                            ELSE TRUE
                        END
    
            """ ).df()
        
        df = con.sql(f"""
        WITH target_zones AS ( 
                    SELECT 
                        id_zone,
                        name_zone,
                        zone_type,
                        visual_point,
                    FROM silver.zones_info
                    WHERE CASE
                        WHEN unhex('{geometry}') IS NOT NULL AND '{geometry}' != '' THEN
                            ST_Intersects(ST_Transform(ST_GeomFromWKB(unhex('{geometry}')), 'EPSG:4326', 'EPSG:25830'),  geometry)
                        ELSE TRUE
                    END
                )

        SELECT 
            t.zone_type,
            t.id_origin,
            zo.name_zone as name_origin,
            t.id_destination,
            zd.name_zone as name_destination,
            ROUND(actual_trips,2) as actual_trips,
            ROUND(theoretical_trips,2) as theoretical_trips,
            ROUND(gap,2) as gap,
            ST_AsWKB(zo.visual_point) as point_origin,
            ST_AsWKB(zd.visual_point) as point_destination,
            

        FROM test.infrastructure_gaps{table_sufix} t
        LEFT JOIN target_zones zo ON zo.id_zone = t.id_origin AND zo.zone_type = t.zone_type
        LEFT JOIN target_zones zd ON zd.id_zone = t.id_destination AND zd.zone_type = t.zone_type

        ORDER BY gap DESC LIMIT 500
            """).df()
        
        for i, zone in enumerate(zones):
            df_geo_dist = df_geo[df_geo["zone_type"] == f"{zone}"]
            df_geo_dist['geometry'] = df_geo_dist['geometry'].apply(lambda x: wkb.loads(bytes(x)))
            gdf_dist = gpd.GeoDataFrame(df_geo_dist, geometry='geometry')
            # ----- DISTRITOS
            df_dist = df[df["zone_type"]==f"{zone}"]
            df_dist['point_origin'] = df_dist['point_origin'].apply(lambda x: wkb.loads(bytes(x)))
            df_dist['point_destination'] = df_dist['point_destination'].apply(lambda x: wkb.loads(bytes(x)))
            df_dist
            df_dist['line'] = df_dist.apply(
                lambda row: LineString([row['point_origin'], row['point_destination']]), 
                axis=1
            )
            df_dist['gap_abs'] = df_dist['gap'].abs()
            df_dist['gap_abs'] = df_dist['gap_abs'].clip(upper=df_dist['gap_abs'].quantile(0.90))
            min_width = 0.5
            max_width = 15.0

            # Normalización Min-Max matemática
            min_gap = df_dist['gap_abs'].min()
            max_gap = df_dist['gap_abs'].max()

            # Fórmula: (Valor - Min) / (Max - Min) * (Rango_Grosor) + Grosor_Base
            df_dist['gap_norm'] = (
                (df_dist['gap_abs'] - min_gap) / (max_gap - min_gap)) * (max_width - min_width) + min_width
            
      
            fig, axes = plt.subplots(figsize=(16, 16))

            gdf_dist.plot(
                ax=axes, 
                color='lightblue', 
                edgecolor='blue', 
                alpha=0.3, 
                linewidth=0.6,
                aspect=None
            )
            gpd.GeoSeries(df_dist['line']).plot(
                ax=axes,
                color=colores[i % len(colores)], 
                linewidth=df_dist['gap_norm'],
                alpha=0.5,
                label=f'{zone} gap'
            )
            axes.set_title(f"Infrastructure Gaps - {zone} ({start_short}-{end_short})")
            axes.set_xlabel("Longitude")
            axes.set_ylabel("Latitude")

            # Ajuste final para que no se solapen
            plt.tight_layout()
            output_dir = os.getcwd() + "/dags/output_plots/infa_gap"
                
            # Nombre del archivo
            filename = f"{output_dir}/Infraestructure_gaps_{zone}_{start_short}-{end_short}.png"
            
            # Guardar
            #print(f"Guardando gráfico en: {filename}")
            plt.savefig(filename, bbox_inches='tight')
            plt.close()    

            # GRAFICO BARRAS
            df_top = df_dist.sort_values(by='gap', ascending=False).head(10)

            # 2. Función de limpieza de nombres
            def clean_name(name):
                if pd.isna(name): return ""
                # Eliminamos la coletilla que hace el nombre largo
                return name.replace(' agregacion de municipios', '')

            # Aplicar limpieza
            df_top['origin_clean'] = df_top['name_origin'].apply(clean_name)
            df_top['dest_clean'] = df_top['name_destination'].apply(clean_name)

            # 3. Crear etiquetas formateadas (con salto de línea para ahorrar espacio horizontal)
            # Usamos una flecha simple '->' o un salto '\n->\n'
            df_top['route_label'] = df_top['origin_clean'] + "\n- " + df_top['dest_clean']

            # 4. Configuración del Gráfico
            labels = df_top['route_label']
            actual = df_top['actual_trips']
            theoretical = df_top['theoretical_trips']

            x = np.arange(len(labels))
            width = 0.35  # Ancho de barras

            fig, ax = plt.subplots(figsize=(14, 8)) # Un poco más ancho para los nombres

            # Barras
            rects1 = ax.bar(x - width/2, actual, width, label='Actual Trips', color='#3498db') # Azul
            rects2 = ax.bar(x + width/2, theoretical, width, label='Theoretical Trips', color='#e67e22') # Naranja

            # --- Estilos y Escala ---
            ax.set_yscale('log') # Escala Logarítmica
            ax.set_ylabel('Number of Trips (Log Scale)', fontsize=12)
            ax.set_title(f'{zone} Top Infrastructure Gaps: Actual vs Theoretical ({start_short}-{end_short})', fontsize=16, pad=20)

            # Eje X
            ax.set_xticks(x)
            ax.set_xticklabels(labels, rotation=45, ha='right', fontsize=10)

            # Leyenda y Grid
            ax.legend(fontsize=12)
            ax.grid(True, which="major", axis='y', linestyle='--', alpha=0.3)

            plt.tight_layout()

            output_dir = os.getcwd() + "/dags/output_plots/top_gap"
                
            # Nombre del archivo
            filename = f"{output_dir}/top_gaps_{zone}_{start_short}-{end_short}.png"
            
            # Guardar
            #print(f"Guardando gráfico en: {filename}")
            plt.savefig(filename, bbox_inches='tight')
            plt.close() 








    test_init = gold_schema()
    day_clusters = build_day_clusters(year=2023,n_clusters=2)
    
    pattern = build_typical_pattern_sql()
    
    typical_day_pattern = BatchOperator.partial(
        task_id='test-pattern',
        job_name='test-pattern',
        job_queue='duck_jobque',
        job_definition='duck_jobdef',
        region_name='eu-central-1',
        submit_job_timeout= 1200,
        # Opcional: Aumentar timeout porque son cargas pesadas
        
    ).expand(container_overrides=pattern)
    

    plot = hourly_plots(zone =["gaus","municiples","districts"] )
    mapa = map_plots(zone =["gaus","municiples","districts"])

    test_init >> day_clusters
    day_clusters >> pattern
    typical_day_pattern >> plot
    typical_day_pattern >> mapa

    gravity_pair = sql_gravity_pair( zones =["gaus","municiples","districts"])
    batch_gravity_pair = BatchOperator.partial(
        task_id='gravity_pair',
        job_name='gravity_pair',
        job_queue='duck_jobque',
        job_definition='duck_jobdef',
        region_name='eu-central-1',
        submit_job_timeout= 1200,
        # Opcional: Aumentar timeout porque son cargas pesadas
        
    ).expand(container_overrides=gravity_pair)

    fit_k = fit_gravity_k()
    infrastructure_gap = infrastructure_gap_sql()
    mapa_gap = gap_plots(zones=["gaus","municiples","districts"])

    test_init >> gravity_pair
    batch_gravity_pair >> fit_k
    fit_k >> infrastructure_gap
    infrastructure_gap >> mapa_gap

gold1 = bq1_dag()
