

from __future__ import annotations

import numpy as np
import pandas as pd

from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

# BUSINESS QUESTION 1 (GOLD): Typical Day & Mobility Patterns (Clustering)

"""
El objetivo es agrupar los días del año en tipos de día según su perfil horario de movilidad
 """

def build_gold_typical_day_patterns(
    con,
    year: int = 2023,
    n_clusters: int = 3, #cuantos tipos de día queremos
    zone_types: list[str] | None = None,
    include_volume_in_clustering: bool = True,
    random_state: int = 42,
) -> None:
    """
    herramientas (tablas) GOLD para Business Question 1:
      1) gold.day_features  : saca de silver una serie temporal por día y hora por zone_type
      2) gold.day_clusters  : k-means por cada zone_type
      3) gold.typical_day_patterns : average hourly OD matrices by pattern type

    """
   
    con.sql("CREATE SCHEMA IF NOT EXISTS gold;")

    
    # Extract hourly totals per day and zone_type 
    zone_filter_sql = ""
    if zone_types:
        zone_list = ", ".join([f"'{z}'" for z in zone_types])
        zone_filter_sql = f"AND zone_type IN ({zone_list})"

    
    #para clusterizar días necesitas describir cada día como un vector de 24 valores
    df_temporal = con.sql(f"""
        SELECT
            CAST(date AS DATE) AS trip_date,
            zone_type,
            EXTRACT(HOUR FROM date) AS hour_of_day,
            SUM(n_trips) AS total_trips
        FROM silver.od_trips
        WHERE YEAR(date) = {year}
        {zone_filter_sql}
        GROUP BY 1,2,3
        ORDER BY 1,2,3
    """).df()

    if df_temporal.empty:
        print("No data in silver.od_trips fpr this year/especifications.")
        return


    #Pivot day x hour per zone_type and build FEATURES
    pivot = (
        df_temporal
        .pivot_table(
            index=["trip_date", "zone_type"],
            columns="hour_of_day",
            values="total_trips",
            aggfunc="sum",
            fill_value=0.0
        )
        .sort_index()
    )

    # Ensure all 24 hours exist as columns
    all_hours = list(range(24))
    pivot = pivot.reindex(columns=all_hours, fill_value=0.0)

    # Daily total volume
    daily_total = pivot.sum(axis=1)

    # Row-wise normalization (distribution over hours)
    # If daily_total == 0 -> keep zeros
    shares = pivot.div(daily_total.replace(0, np.nan), axis=0).fillna(0.0)

    
    peak_hour = pivot.idxmax(axis=1).astype(int) # hora de max vol en valores reales
    morning_share = shares.loc[:, 7:10].sum(axis=1)    # % de viajes de 7-10
    evening_share = shares.loc[:, 17:20].sum(axis=1)   # % de viajes de 17-20

    # Build feature dataframe
    df_features = shares.copy()
    df_features.columns = [f"share_h{h:02d}" for h in df_features.columns]

    df_features = df_features.reset_index()
    df_features["total_trips"] = daily_total.values
    df_features["peak_hour"] = peak_hour.values
    df_features["morning_share"] = morning_share.values
    df_features["evening_share"] = evening_share.values

    # Calendar features for labeling
    df_features["weekday"] = pd.to_datetime(df_features["trip_date"]).dt.weekday  # Mon=0
    df_features["is_weekend"] = (df_features["weekday"] >= 5)

    # gold.day_features - esta tabla intermedia ayuda con la trazabilidad y reutilización
    df_features_out = df_features.copy()
    df_features_out["ingestion_date"] = pd.Timestamp.utcnow()

    con.register("df_day_features", df_features_out)
    con.sql("CREATE OR REPLACE TABLE gold.day_features AS SELECT * FROM df_day_features;")
    con.unregister("df_day_features")

   #---------------
    #KMeans PER zone_type (per zone level)
   
    share_cols = [c for c in df_features.columns if c.startswith("share_h")]
    cluster_rows = []

    #bucle por zone_type -cada nivel (districts/municiples/gaus) puede tener escala y cobertura distintas 
    # clusterizar por separado evita mezclar patrones.
    for zt, df_zt in df_features.groupby("zone_type", sort=True):
        df_zt = df_zt.copy()

        # not enough days
        n_days = len(df_zt)
        k = min(n_clusters, n_days)  # if n_days < n_clusters
        if k < 2:
            # no clustering possible
            df_zt["cluster_id"] = 0
            df_zt["pattern_name"] = "Single pattern (insufficient days)"
            cluster_rows.append(df_zt[["trip_date", "zone_type", "cluster_id", "pattern_name"]])
            continue

        # Feature matrix for clustering
        X_parts = [df_zt[share_cols].to_numpy(dtype=float)]

        # We keep shape as core, and optionally add stabilized volume + peak.
        if include_volume_in_clustering:
            vol = np.log1p(df_zt["total_trips"].to_numpy(dtype=float)).reshape(-1, 1) #log1p nos ayuda a estab la escala
            pk = df_zt["peak_hour"].to_numpy(dtype=float).reshape(-1, 1)
            X_parts += [vol, pk]

        X = np.hstack(X_parts)

        # Standardize for KMeans stability
        X_scaled = StandardScaler().fit_transform(X)

        km = KMeans(
            n_clusters=k,
            random_state=random_state,
            n_init=10,
        )
        df_zt["cluster_id"] = km.fit_predict(X_scaled)

       
        # Etiquetado de clusters
        df_zt["is_weekday"] = (df_zt["weekday"] <= 4)

        stats = (
            df_zt.groupby("cluster_id")
            .agg(
                weekday_rate=("is_weekday", "mean"),
                avg_total_trips=("total_trips", "mean"),
                avg_morning_share=("morning_share", "mean"),
                avg_evening_share=("evening_share", "mean"),
                n_days=("trip_date", "count"),
            )
            .sort_values(["weekday_rate", "avg_total_trips"], ascending=[True, True])
        )

        """
        cluster con menor weekday_rate ⇒ “Weekend-like”
        cluster con mayor weekday_rate ⇒ “Weekday-like”
        intermedios ⇒ “Mixed/Holiday-like”
        """
        label_map = {}
        cluster_ids_sorted = list(stats.index)

        if len(cluster_ids_sorted) == 2:
            label_map[cluster_ids_sorted[0]] = "Weekend-like"
            label_map[cluster_ids_sorted[1]] = "Weekday-like"
        else:
            label_map[cluster_ids_sorted[0]] = "Weekend-like"
            label_map[cluster_ids_sorted[-1]] = "Weekday-like"
            for cid in cluster_ids_sorted[1:-1]:
                label_map[cid] = "Mixed/Holiday-like"

        df_zt["pattern_name"] = df_zt["cluster_id"].map(label_map).fillna(
            "Pattern " + df_zt["cluster_id"].astype(str)
        )

        cluster_rows.append(df_zt[["trip_date", "zone_type", "cluster_id", "pattern_name"]])

        print(f"  zone_type={zt}: clustered {n_days} days into k={k} clusters")
        print(stats)

    df_clusters = pd.concat(cluster_rows, ignore_index=True)
    df_clusters["ingestion_date"] = pd.Timestamp.utcnow()

    #  gold.day_clusters - mapeo (día, zone_type) → (cluster_id, pattern_name) (diccionario)
    con.register("df_day_clusters", df_clusters)
    con.sql("CREATE OR REPLACE TABLE gold.day_clusters AS SELECT * FROM df_day_clusters;")
    con.unregister("df_day_clusters")

   #------------------
    # gold.typical_day_patterns - para que el promedio sea “promedio por día”
    con.sql(f"""
        CREATE OR REPLACE TEMP TABLE daily_od_hour AS
        SELECT
            CAST(t.date AS DATE) AS trip_date,
            t.zone_type AS zone_type,
            EXTRACT(HOUR FROM t.date) AS hour_of_day,
            t.id_origin,
            t.id_destination,

            -- Per-day aggregation (this is what you must average later)
            SUM(t.n_trips) AS daily_trips,
            SUM(t.trips_total_length_km) AS daily_total_length_km,
            SUM(t.trips_total_length_km) / NULLIF(SUM(t.n_trips), 0) AS daily_avg_trip_length_km
        FROM silver.od_trips t
        WHERE YEAR(t.date) = {year}
        {zone_filter_sql}
        GROUP BY 1,2,3,4,5
    """)
    #tabla final de BQ1
    con.sql("""
        CREATE OR REPLACE TABLE gold.typical_day_patterns AS
        SELECT
            dc.zone_type,
            dc.cluster_id,
            dc.pattern_name,
            d.hour_of_day,
            d.id_origin,
            d.id_destination,

            -- "Typical day" metrics = averages across DAYS in the cluster
            AVG(d.daily_trips) AS avg_trips_per_day,
            AVG(d.daily_avg_trip_length_km) AS avg_trip_length_km,

            -- Demand totals (useful for the article / sanity checks)
            SUM(d.daily_trips) AS total_trips_in_cluster_sample,
            COUNT(DISTINCT d.trip_date) AS n_days_in_cluster,

            CURRENT_TIMESTAMP AS ingestion_date
        FROM daily_od_hour d
        JOIN gold.day_clusters dc
          ON d.trip_date = dc.trip_date
         AND d.zone_type = dc.zone_type
        GROUP BY
            dc.zone_type, dc.cluster_id, dc.pattern_name,
            d.hour_of_day, d.id_origin, d.id_destination
    """)

    print("\nGold tables created/updated:")
    con.sql("SELECT COUNT(*) AS rows FROM gold.day_features").show()
    con.sql("SELECT zone_type, COUNT(*) AS rows FROM gold.day_clusters GROUP BY 1 ORDER BY 1").show()
    con.sql("""
        SELECT zone_type, pattern_name, COUNT(*) AS rows
        FROM gold.typical_day_patterns
        GROUP BY 1,2
        ORDER BY zone_type, rows DESC
    """).show()

    print("\nDone: gold.day_features, gold.day_clusters, gold.typical_day_patterns")



# BUSINESS QUESTION 2: Infrastructure Gaps (Gravity Model)

#Identificar dónde falta infraestructura/conectividad comparando viajes reales vs viajes esperados

def build_gold_infrastructure_gaps(
    con,
    year: int = 2023,
    zone_level: str = "districts",
    dist_floor_km: float = 0.1,
    min_actual_trips_preview: int = 50,
    create_zone_ranking: bool = True,
) -> None:
    """
      - gold.zone_year_attributes - Atributos por zona MITMA y año
      - gold.gravity_params       - Ajuste del parámetro k:
      - gold.infrastructure_gaps  - Tabla principal de gaps:actual vs estimated 
      - gold.zone_gap_ranking     - Ranking por zona
    """

    # Normalize user input - evitar errores
    zone_dic = {"municipios": "municiples", "distritos": "districts", "gau": "gaus"}
    zone_type = zone_dic.get(zone_level.lower(), zone_level).lower()

    valid = {"districts", "municiples", "gaus"}
    if zone_type not in valid:
        raise ValueError(f"zone_level/zone_type must be one of {sorted(valid)} (got: {zone_type})")

    # Map column in silver.ine_mitma_zones to the target MITMA zone id
    target_col = {
        "districts": "id_districts_mitma",
        "municiples": "id_municiples_mitma",
        "gaus": "id_gaus_mitma",
    }[zone_type]

    con.sql("CREATE SCHEMA IF NOT EXISTS gold;")

    #INE y MITMA no usan los mismos ids. Los transformamos para poder usarlos
    con.sql("""
        CREATE TABLE IF NOT EXISTS gold.zone_year_attributes (
            zone_type VARCHAR,
            year INTEGER,
            id_zone INTEGER,
            population DOUBLE,
            economic_activity_proxy DOUBLE,
            ingestion_date TIMESTAMP
        );
    """)

    # evitar duplicados
    con.sql(f"""
        DELETE FROM gold.zone_year_attributes
        WHERE zone_type = '{zone_type}' AND year = {year};
    """)

    # Insert correct attributes slice
    con.sql(f"""
        INSERT INTO gold.zone_year_attributes
        WITH
        pop_sec AS (
            SELECT
                id_zone AS id_section,
                year,
                population
            FROM silver.spain_population
            WHERE year = {year}
        ),
        rent_ine AS (
            SELECT
                id_zone AS id_district_ine,
                year,
                rent
            FROM silver.average_rent
            WHERE year = {year}
        ),
        map AS (
            SELECT
                id_sections_ine,
                id_districts_ine,
                {target_col} AS id_target_mitma
            FROM silver.ine_mitma_zones
            WHERE {target_col} IS NOT NULL
        ),

        -- Population per MITMA zone (sum sections)
        pop_target AS (
            SELECT
                '{zone_type}' AS zone_type,
                p.year,
                m.id_target_mitma AS id_zone,
                SUM(p.population) AS population
            FROM pop_sec p
            JOIN map m ON p.id_section = m.id_sections_ine
            GROUP BY 1,2,3
        ),

        -- Economic proxy per MITMA zone
        -- For districts: map district_ine -> district_mitma, essentially direct
        rent_target_districts AS (
            SELECT
                'districts' AS zone_type,
                r.year,
                m.id_target_mitma AS id_zone,
                AVG(r.rent) AS economic_activity_proxy
            FROM rent_ine r
            JOIN map m ON r.id_district_ine = m.id_districts_ine
            GROUP BY 1,2,3
        ),

        -- For municiples/gaus: compute weighted rent via sections:
        -- each section belongs to an INE district -> has rent; aggregate to target by pop weights
        rent_target_weighted AS (
            SELECT
                '{zone_type}' AS zone_type,
                p.year,
                m.id_target_mitma AS id_zone,
                SUM(p.population * r.rent) / NULLIF(SUM(p.population), 0) AS economic_activity_proxy
            FROM pop_sec p
            JOIN map m ON p.id_section = m.id_sections_ine
            JOIN rent_ine r ON m.id_districts_ine = r.id_district_ine AND p.year = r.year
            GROUP BY 1,2,3
        ),

        rent_target AS (
            SELECT * FROM rent_target_districts WHERE zone_type = '{zone_type}'
            UNION ALL
            SELECT * FROM rent_target_weighted
        ),

        defaults AS (
            SELECT
                AVG(population) AS avg_pop,
                AVG(economic_activity_proxy) AS avg_econ
            FROM (
                SELECT p.population, rt.economic_activity_proxy
                FROM pop_target p
                LEFT JOIN rent_target rt
                  ON p.zone_type = rt.zone_type AND p.year = rt.year AND p.id_zone = rt.id_zone
            )
        )
        SELECT
            p.zone_type,
            p.year,
            p.id_zone,
            COALESCE(p.population, d.avg_pop, 0) AS population,
            COALESCE(rt.economic_activity_proxy, d.avg_econ, 0) AS economic_activity_proxy,
            CURRENT_TIMESTAMP AS ingestion_date
        FROM pop_target p
        LEFT JOIN rent_target rt
          ON p.zone_type = rt.zone_type AND p.year = rt.year AND p.id_zone = rt.id_zone
        CROSS JOIN defaults d;
    """)

    #guardamos el parámetro de calibración k del modelo gravitatorio para un año y un nivel de zona concretos.
    con.sql("""
        CREATE TABLE IF NOT EXISTS gold.gravity_params (
            zone_type VARCHAR,
            year INTEGER,
            k DOUBLE,
            n_pairs_used BIGINT,
            fitted_at TIMESTAMP
        );
    """)

    con.sql(f"DELETE FROM gold.gravity_params WHERE zone_type = '{zone_type}' AND year = {year};")

    con.sql(f"""
        INSERT INTO gold.gravity_params
        WITH
        od AS (
            SELECT
                TRY_CAST(id_origin AS INTEGER) AS id_origin,
                TRY_CAST(id_destination AS INTEGER) AS id_destination,
                SUM(n_trips) AS actual_trips
            FROM silver.od_trips
            WHERE zone_type = '{zone_type}' AND YEAR(date) = {year}
            GROUP BY 1,2
        ),
        dist AS (
            SELECT
                zone_type,
                id_origin AS a,
                id_destination AS b,
                distance_km
            FROM silver.zone_pairs
            WHERE zone_type = '{zone_type}'
        ),
        attr AS (
            SELECT id_zone, population, economic_activity_proxy
            FROM gold.zone_year_attributes
            WHERE zone_type = '{zone_type}' AND year = {year}
        ),
        feats AS (
            SELECT
                o.id_origin,
                o.id_destination,
                o.actual_trips,
                d.distance_km,
                ao.population AS pop_origin,
                ad.economic_activity_proxy AS econ_dest,
                (ao.population * ad.economic_activity_proxy) /
                    NULLIF(POWER(GREATEST(d.distance_km, {dist_floor_km}), 2), 0) AS x_ij
            FROM od o
            JOIN dist d
              ON LEAST(o.id_origin, o.id_destination) = d.a
             AND GREATEST(o.id_origin, o.id_destination) = d.b
            JOIN attr ao ON o.id_origin = ao.id_zone
            JOIN attr ad ON o.id_destination = ad.id_zone
            WHERE d.distance_km IS NOT NULL
        )
        SELECT
            '{zone_type}' AS zone_type,
            {year} AS year,
            SUM(x_ij * actual_trips) / NULLIF(SUM(x_ij * x_ij), 0) AS k,
            COUNT(*) AS n_pairs_used,
            CURRENT_TIMESTAMP AS fitted_at
        FROM feats
        WHERE x_ij IS NOT NULL AND x_ij > 0 AND actual_trips IS NOT NULL;
    """)

   
    # Tabla final gold.infrastructure_gaps
    con.sql("""
        CREATE TABLE IF NOT EXISTS gold.infrastructure_gaps (
            id_origin INTEGER,
            id_destination INTEGER,
            zone_type VARCHAR,
            year INTEGER,
            distance_km DOUBLE,
            actual_trips DOUBLE,
            theoretical_trips DOUBLE,
            mismatch_ratio DOUBLE,
            gap DOUBLE,
            ingestion_date TIMESTAMP
        );
    """)

    con.sql(f"DELETE FROM gold.infrastructure_gaps WHERE zone_type = '{zone_type}' AND year = {year};")

    con.sql(f"""
        INSERT INTO gold.infrastructure_gaps
        WITH
        kpar AS (
            SELECT k
            FROM gold.gravity_params
            WHERE zone_type = '{zone_type}' AND year = {year}
        ),
        od AS (
            SELECT
                TRY_CAST(id_origin AS INTEGER) AS id_origin,
                TRY_CAST(id_destination AS INTEGER) AS id_destination,
                SUM(n_trips) AS actual_trips
            FROM silver.od_trips
            WHERE zone_type = '{zone_type}' AND YEAR(date) = {year}
            GROUP BY 1,2
        ),
        dist AS (
            SELECT
                id_origin AS a,
                id_destination AS b,
                distance_km
            FROM silver.zone_pairs
            WHERE zone_type = '{zone_type}'
        ),
        attr AS (
            SELECT id_zone, population, economic_activity_proxy
            FROM gold.zone_year_attributes
            WHERE zone_type = '{zone_type}' AND year = {year}
        ),
        feats AS (
            SELECT
                o.id_origin,
                o.id_destination,
                o.actual_trips,
                d.distance_km,
                (ao.population * ad.economic_activity_proxy) /
                    NULLIF(POWER(GREATEST(d.distance_km, {dist_floor_km}), 2), 0) AS x_ij
            FROM od o
            JOIN dist d
              ON LEAST(o.id_origin, o.id_destination) = d.a
             AND GREATEST(o.id_origin, o.id_destination) = d.b
            JOIN attr ao ON o.id_origin = ao.id_zone
            JOIN attr ad ON o.id_destination = ad.id_zone
            WHERE d.distance_km IS NOT NULL
        )
        SELECT
            f.id_origin,
            f.id_destination,
            '{zone_type}' AS zone_type,
            {year} AS year,
            f.distance_km,
            f.actual_trips,
            (k.k * f.x_ij) AS theoretical_trips,
            f.actual_trips / NULLIF((k.k * f.x_ij), 0) AS mismatch_ratio,
            GREATEST((k.k * f.x_ij) - f.actual_trips, 0) AS gap,
            CURRENT_TIMESTAMP AS ingestion_date
        FROM feats f
        CROSS JOIN kpar k
        WHERE f.x_ij IS NOT NULL AND f.x_ij > 0;
    """)

    print("Gold table created/updated: gold.infrastructure_gaps")

   
    #zone ranking 
    if create_zone_ranking:
        con.sql("""
            CREATE TABLE IF NOT EXISTS gold.zone_gap_ranking (
                zone_type VARCHAR,
                year INTEGER,
                id_zone INTEGER,
                gap_outgoing DOUBLE,
                gap_incoming DOUBLE,
                weighted_gap DOUBLE,
                rank_worst_served BIGINT,
                ingestion_date TIMESTAMP
            );
        """)
        con.sql(f"DELETE FROM gold.zone_gap_ranking WHERE zone_type = '{zone_type}' AND year = {year};")

        con.sql(f"""
            INSERT INTO gold.zone_gap_ranking
            WITH
            out_gap AS (
                SELECT zone_type, year, id_origin AS id_zone, SUM(gap) AS gap_outgoing
                FROM gold.infrastructure_gaps
                WHERE zone_type = '{zone_type}' AND year = {year}
                GROUP BY 1,2,3
            ),
            in_gap AS (
                SELECT zone_type, year, id_destination AS id_zone, SUM(gap) AS gap_incoming
                FROM gold.infrastructure_gaps
                WHERE zone_type = '{zone_type}' AND year = {year}
                GROUP BY 1,2,3
            ),
            attr AS (
                SELECT id_zone, population, economic_activity_proxy
                FROM gold.zone_year_attributes
                WHERE zone_type = '{zone_type}' AND year = {year}
            ),
            merged AS (
                SELECT
                    COALESCE(o.zone_type, i.zone_type) AS zone_type,
                    COALESCE(o.year, i.year) AS year,
                    COALESCE(o.id_zone, i.id_zone) AS id_zone,
                    COALESCE(o.gap_outgoing, 0) AS gap_outgoing,
                    COALESCE(i.gap_incoming, 0) AS gap_incoming
                FROM out_gap o
                FULL JOIN in_gap i
                  ON o.zone_type = i.zone_type AND o.year = i.year AND o.id_zone = i.id_zone
            )
            SELECT
                m.zone_type,
                m.year,
                m.id_zone,
                m.gap_outgoing,
                m.gap_incoming,
                (m.gap_outgoing + m.gap_incoming)
                    * COALESCE(a.population, 1) AS weighted_gap,
                RANK() OVER (
                    PARTITION BY m.zone_type, m.year
                    ORDER BY (m.gap_outgoing + m.gap_incoming) DESC
                ) AS rank_worst_served,
                CURRENT_TIMESTAMP AS ingestion_date
            FROM merged m
            LEFT JOIN attr a ON m.id_zone = a.id_zone;
        """)

        print("Gold table created/updated: gold.zone_gap_ranking")

   #Prueba
    print("\nTop Infrastructure Gaps (low mismatch_ratio):")
    con.sql(f"""
        SELECT *
        FROM gold.infrastructure_gaps
        WHERE zone_type = '{zone_type}' AND year = {year}
          AND actual_trips >= {min_actual_trips_preview}
        ORDER BY mismatch_ratio ASC
        LIMIT 10
    """).show()

    print("\nParams used:")
    con.sql(f"""
        SELECT *
        FROM gold.gravity_params
        WHERE zone_type = '{zone_type}' AND year = {year}
    """).show()

