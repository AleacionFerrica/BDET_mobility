from __future__ import annotations

import duckdb
import numpy as np
import pandas as pd

from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler


#Funciones aux

def _ensure_gold_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.sql("CREATE SCHEMA IF NOT EXISTS gold;")


def _get_zone_types(con: duckdb.DuckDBPyConnection, year: int) -> list[str]:
    rows = con.sql(
        f"""
        SELECT DISTINCT zone_type
        FROM silver.od_trips
        WHERE YEAR(date) = {year}
        ORDER BY 1
        """
    ).fetchall()
    return [r[0] for r in rows]

#---------------------------------------BQ1----------------------------------------

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
def build_day_clusters(
    con: duckdb.DuckDBPyConnection,
    year: int = 2023,
    n_clusters: int = 3,
    zone_types: list[str] | None = None,
    include_volume_in_clustering: bool = True,
    random_state: int = 42,
) -> None:
    _ensure_gold_schema(con)

    if zone_types is None:
        zone_types = _get_zone_types(con, year)

    if not zone_types:
        print(f"[BQ1] No zone_types found for year={year}.")
        return

    zone_list_sql = ", ".join([f"'{z}'" for z in zone_types])

    df_temporal = con.sql(
        f"""
        SELECT
            CAST(date AS DATE) AS trip_date,
            zone_type,
            EXTRACT(HOUR FROM date) AS hour_of_day,
            SUM(n_trips) AS total_trips
        FROM silver.od_trips
        WHERE YEAR(date) = {year}
          AND zone_type IN ({zone_list_sql})
        GROUP BY 1,2,3
        ORDER BY 1,2,3
        """
    ).df()

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

    share_cols = [c for c in df_features.columns if c.startswith("share_h")]

    out_rows: list[pd.DataFrame] = []

    for zt in zone_types:
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
    df_clusters["ingestion_date"] = pd.Timestamp.utcnow()

    con.register("df_day_clusters", df_clusters)
    con.sql("CREATE OR REPLACE TABLE gold.day_clusters AS SELECT * FROM df_day_clusters;")
    con.unregister("df_day_clusters")

    print("[BQ1] gold.day_clusters created. Days per cluster:")
    con.sql(
        """
        SELECT zone_type, cluster_id, pattern_name, COUNT(*) AS n_days
        FROM gold.day_clusters
        GROUP BY 1,2,3
        ORDER BY zone_type, n_days DESC
        """
    ).show()

#typical day patterns - tabla principal
def build_typical_day_patterns(
    con: duckdb.DuckDBPyConnection,
    year: int = 2023,
    zone_types: list[str] | None = None,
) -> None:
    _ensure_gold_schema(con)

    if zone_types is None:
        zone_types = _get_zone_types(con, year)

    if not zone_types:
        print(f"[BQ1] No zone_types found for year={year}.")
        return

    zone_list_sql = ", ".join([f"'{z}'" for z in zone_types])

    con.sql(
        f"""
        CREATE OR REPLACE TEMP TABLE _daily_od_hour AS
        SELECT
            CAST(t.date AS DATE) AS trip_date,
            t.zone_type,
            EXTRACT(HOUR FROM t.date) AS hour_of_day,

            TRY_CAST(t.id_origin AS INTEGER) AS id_origin,
            TRY_CAST(t.id_destination AS INTEGER) AS id_destination,

            SUM(t.n_trips) AS daily_trips,
            SUM(t.trips_total_length_km) AS daily_total_length_km,
            SUM(t.trips_total_length_km) / NULLIF(SUM(t.n_trips), 0) AS daily_avg_trip_length_km
        FROM silver.od_trips t
        WHERE YEAR(t.date) = {year}
          AND t.zone_type IN ({zone_list_sql})
        GROUP BY 1,2,3,4,5
        """
    )

    con.sql("DELETE FROM _daily_od_hour WHERE id_origin IS NULL OR id_destination IS NULL;")

    con.sql(
        """
        CREATE OR REPLACE TABLE gold.typical_day_patterns AS
        SELECT
            dc.zone_type,
            dc.cluster_id,
            dc.pattern_name,
            d.hour_of_day,
            d.id_origin,
            d.id_destination,

            AVG(d.daily_trips) AS avg_trips_per_day,
            AVG(d.daily_avg_trip_length_km) AS avg_trip_length_km,

            SUM(d.daily_trips) AS total_trips_in_cluster_sample,
            COUNT(DISTINCT d.trip_date) AS n_days_in_cluster,

            CURRENT_TIMESTAMP AS ingestion_date
        FROM _daily_od_hour d
        JOIN gold.day_clusters dc
          ON d.trip_date = dc.trip_date
         AND d.zone_type = dc.zone_type
        GROUP BY
            dc.zone_type, dc.cluster_id, dc.pattern_name,
            d.hour_of_day, d.id_origin, d.id_destination
        """
    )

    print("[BQ1] gold.typical_day_patterns created. Rows per pattern:")
    con.sql(
        """
        SELECT zone_type, pattern_name, COUNT(*) AS rows
        FROM gold.typical_day_patterns
        GROUP BY 1,2
        ORDER BY zone_type, rows DESC
        """
    ).show()


def run_bq1(
    con: duckdb.DuckDBPyConnection,
    year: int = 2023,
    n_clusters: int = 3,
    zone_types: list[str] | None = None,
    include_volume_in_clustering: bool = True,
    random_state: int = 42,
) -> None:
    build_day_clusters(
        con=con,
        year=year,
        n_clusters=n_clusters,
        zone_types=zone_types,
        include_volume_in_clustering=include_volume_in_clustering,
        random_state=random_state,
    )
    build_typical_day_patterns(
        con=con,
        year=year,
        zone_types=zone_types,
    )


#--------------------------------------- BQ2 --------------------------------------------

#Funciones auxiliares
def _normalize_zone_type(zone_type: str) -> str:
    z = zone_type.strip().lower()
    aliases = {"distritos": "districts", "municipios": "municiples", "gau": "gaus"}
    z = aliases.get(z, z)

    valid = {"districts", "municiples", "gaus"}
    if z not in valid:
        raise ValueError(f"zone_type must be one of {sorted(valid)} (got: {zone_type})")
    return z


def _zone_types_sql(zone_types: list[str]) -> str:
    return ", ".join([f"'{_normalize_zone_type(z)}'" for z in zone_types])


def build_gravity_pair_features(
    con: duckdb.DuckDBPyConnection,
    year: int = 2023,
    zone_type: str = "districts",
    dist_floor_km: float = 0.1,
    dist_power: int = 2,
    keep_only_with_distance: bool = True,
) -> None:
    _ensure_gold_schema(con)
    zt = _normalize_zone_type(zone_type)

    con.sql("""
        CREATE TABLE IF NOT EXISTS gold.gravity_pair_features (
            zone_type VARCHAR,
            year INTEGER,
            id_origin INTEGER,
            id_destination INTEGER,
            distance_km DOUBLE,
            actual_trips DOUBLE,
            pop_origin DOUBLE,
            rent_dest DOUBLE,
            x_ij DOUBLE,
            ingestion_date TIMESTAMP
        );
    """)
    #x_ij= (P_i * E_j) / d^power, es el "score gravitatorio", es una proporción, si nos fijamos en la fórmula del documento
    #todavía falta un parámetro k para poder calcular los viajes teóricos


    con.sql(f"DELETE FROM gold.gravity_pair_features WHERE zone_type = '{zt}' AND year = {year};")

    con.sql(
        f"""
        INSERT INTO gold.gravity_pair_features
        WITH
        od AS (
            SELECT
                TRY_CAST(id_origin AS INTEGER) AS id_origin,
                TRY_CAST(id_destination AS INTEGER) AS id_destination,
                SUM(n_trips) AS actual_trips
            FROM silver.od_trips
            WHERE zone_type = '{zt}'
              AND YEAR(date) = {year}
            GROUP BY 1,2
        ),
        od_clean AS (
            SELECT *
            FROM od
            WHERE id_origin IS NOT NULL AND id_destination IS NOT NULL
              AND id_origin <> id_destination
              AND actual_trips IS NOT NULL
        ),
        dist AS (
            SELECT
                zone_type,
                id_origin AS a,
                id_destination AS b,
                distance_km
            FROM silver.zone_pairs
            WHERE zone_type = '{zt}'
        ),
        pop AS (
            SELECT
                dz.id_zone,
                COALESCE(p.population, (SELECT AVG(population) FROM silver.spain_population WHERE year={year})) AS population
            FROM silver.dim_zones dz
            LEFT JOIN silver.spain_population p
              ON p.id_zone = dz.id_zone AND p.year = {year}
            WHERE dz.zone_type = '{zt}'
        ),
        rent AS (
            SELECT
                dz.id_zone,
                COALESCE(r.rent, (SELECT AVG(rent) FROM silver.average_rent WHERE year={year})) AS rent
            FROM silver.dim_zones dz
            LEFT JOIN silver.average_rent r
              ON r.id_zone = dz.id_zone AND r.year = {year}
            WHERE dz.zone_type = '{zt}'
        ),
        joined AS (
            SELECT
                '{zt}' AS zone_type,
                {year} AS year,
                o.id_origin,
                o.id_destination,
                d.distance_km,
                o.actual_trips,
                po.population AS pop_origin,
                rd.rent AS rent_dest
            FROM od_clean o
            LEFT JOIN dist d
              ON LEAST(o.id_origin, o.id_destination) = d.a
             AND GREATEST(o.id_origin, o.id_destination) = d.b
            LEFT JOIN pop po ON o.id_origin = po.id_zone
            LEFT JOIN rent rd ON o.id_destination = rd.id_zone
        )
        SELECT
            zone_type,
            year,
            id_origin,
            id_destination,
            distance_km,
            actual_trips,
            pop_origin,
            rent_dest,
            (pop_origin * rent_dest)
              / NULLIF(POWER(GREATEST(distance_km, {dist_floor_km}), {dist_power}), 0) AS x_ij,
            CURRENT_TIMESTAMP AS ingestion_date
        FROM joined
        {"WHERE distance_km IS NOT NULL" if keep_only_with_distance else ""}
        ;
        """
    )

    print(f"[BQ2] gold.gravity_pair_features built for zone_type={zt}, year={year}.")
    con.sql(
        f"""
        SELECT
          zone_type, year,
          COUNT(*) AS n_pairs,
          SUM(actual_trips) AS total_actual_trips,
          AVG(distance_km) AS avg_distance_km
        FROM gold.gravity_pair_features
        WHERE zone_type='{zt}' AND year={year}
        GROUP BY 1,2
        """
    ).show()


#Calculamos el parámetro k
#Se elabora un cálculo que elige k para que el error cuadrático total sea mínimo

def fit_gravity_k(
    con: duckdb.DuckDBPyConnection,
    year: int = 2023,
    zone_type: str = "districts",
) -> None:
    _ensure_gold_schema(con)
    zt = _normalize_zone_type(zone_type)

    con.sql("""
        CREATE TABLE IF NOT EXISTS gold.gravity_params (
            zone_type VARCHAR,
            year INTEGER,
            k DOUBLE,
            n_pairs_used BIGINT,
            fitted_at TIMESTAMP
        );
    """)
    con.sql(f"DELETE FROM gold.gravity_params WHERE zone_type='{zt}' AND year={year};")

    con.sql(
        f"""
        INSERT INTO gold.gravity_params
        SELECT
            '{zt}' AS zone_type,
            {year} AS year,
            SUM(x_ij * actual_trips) / NULLIF(SUM(x_ij * x_ij), 0) AS k,
            COUNT(*) AS n_pairs_used,
            CURRENT_TIMESTAMP AS fitted_at
        FROM gold.gravity_pair_features
        WHERE zone_type='{zt}' AND year={year}
          AND x_ij IS NOT NULL AND x_ij > 0
          AND actual_trips IS NOT NULL
        ;
        """
    )

    print(f"[BQ2] gold.gravity_params fitted for zone_type={zt}, year={year}.")
    con.sql(f"SELECT * FROM gold.gravity_params WHERE zone_type='{zt}' AND year={year};").show()


#TABLA PRINCIPAL
def build_infrastructure_gaps(
    con: duckdb.DuckDBPyConnection,
    year: int = 2023,
    zone_type: str = "districts",
) -> None:
    _ensure_gold_schema(con)
    zt = _normalize_zone_type(zone_type)

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

    con.sql(f"DELETE FROM gold.infrastructure_gaps WHERE zone_type='{zt}' AND year={year};")

    con.sql(
        f"""
        INSERT INTO gold.infrastructure_gaps
        WITH kpar AS (
            SELECT k
            FROM gold.gravity_params
            WHERE zone_type='{zt}' AND year={year}
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
        CROSS JOIN kpar k
        WHERE f.zone_type='{zt}' AND f.year={year}
          AND f.x_ij IS NOT NULL AND f.x_ij > 0
        ;
        """
    )

    print(f"[BQ2] gold.infrastructure_gaps built for zone_type={zt}, year={year}.")
    print("[BQ2] Top potential gaps (lowest mismatch_ratio, with some volume):")
    con.sql(
        f"""
        SELECT *
        FROM gold.infrastructure_gaps
        WHERE zone_type='{zt}' AND year={year}
          AND actual_trips >= 50
        ORDER BY mismatch_ratio ASC
        LIMIT 10
        """
    ).show()


# Zone ranking - tabla opcional que clasifica las zonas en función del nivel/calidad de servicio - usando los gaps en las infra
def create_zone_gap_ranking_view(con: duckdb.DuckDBPyConnection) -> None:
    _ensure_gold_schema(con)

    con.sql("""
        CREATE OR REPLACE VIEW gold.zone_gap_ranking AS
        WITH
        out_gap AS (
            SELECT zone_type, year, id_origin AS id_zone, SUM(gap) AS gap_outgoing
            FROM gold.infrastructure_gaps
            GROUP BY 1,2,3
        ),
        in_gap AS (
            SELECT zone_type, year, id_destination AS id_zone, SUM(gap) AS gap_incoming
            FROM gold.infrastructure_gaps
            GROUP BY 1,2,3
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
              ON o.zone_type=i.zone_type AND o.year=i.year AND o.id_zone=i.id_zone
        ),
        attrs AS (
            SELECT
                dz.zone_type,
                p.year,
                dz.id_zone,
                COALESCE(p.population, (SELECT AVG(population) FROM silver.spain_population WHERE year=p.year)) AS population,
                COALESCE(r.rent, (SELECT AVG(rent) FROM silver.average_rent WHERE year=p.year)) AS rent
            FROM silver.dim_zones dz
            LEFT JOIN silver.spain_population p
              ON p.id_zone = dz.id_zone
            LEFT JOIN silver.average_rent r
              ON r.id_zone = dz.id_zone AND r.year = p.year
        )
        SELECT
            m.zone_type,
            m.year,
            m.id_zone,
            m.gap_outgoing,
            m.gap_incoming,
            (m.gap_outgoing + m.gap_incoming) AS gap_total,
            a.population,
            a.rent,
            (m.gap_outgoing + m.gap_incoming) * COALESCE(a.population, 1) * (COALESCE(a.rent, 1) / 1000.0) AS weighted_gap,
            RANK() OVER (
                PARTITION BY m.zone_type, m.year
                ORDER BY (m.gap_outgoing + m.gap_incoming) * COALESCE(a.population, 1) * (COALESCE(a.rent, 1) / 1000.0) DESC
            ) AS rank_worst_served
        FROM merged m
        LEFT JOIN attrs a
          ON a.zone_type = m.zone_type AND a.year = m.year AND a.id_zone = m.id_zone
        ;
    """)

    print("[BQ2] gold.zone_gap_ranking VIEW created/updated.")


def run_bq2(
    con: duckdb.DuckDBPyConnection,
    year: int = 2023,
    zone_types: list[str] | None = None,
    dist_floor_km: float = 0.1,
    dist_power: int = 2,
) -> None:
    _ensure_gold_schema(con)

    if zone_types is None:
        zone_types = ["districts", "municiples", "gaus"]

    zone_types_norm = [_normalize_zone_type(z) for z in zone_types]

    for zt in zone_types_norm:
        build_gravity_pair_features(
            con=con,
            year=year,
            zone_type=zt,
            dist_floor_km=dist_floor_km,
            dist_power=dist_power,
            keep_only_with_distance=True,
        )
        fit_gravity_k(con=con, year=year, zone_type=zt)
        build_infrastructure_gaps(con=con, year=year, zone_type=zt)

    create_zone_gap_ranking_view(con)


# Orchestrator (BQ1 + BQ2)
def run_gold_pipeline(
    con: duckdb.DuckDBPyConnection,
    year: int = 2023,
    run_bq1: bool = True,
    run_bq2: bool = True,
    # BQ1 params
    bq1_n_clusters: int = 3,
    bq1_zone_types: list[str] | None = None,
    bq1_include_volume: bool = True,
    # BQ2 params
    bq2_zone_types: list[str] | None = None,
    bq2_dist_floor_km: float = 0.1,
    bq2_dist_power: int = 2,
) -> None:
    if run_bq1:
        print("RUNNING BQ1")
        run_bq1(
            con=con,
            year=year,
            n_clusters=bq1_n_clusters,
            zone_types=bq1_zone_types,
            include_volume_in_clustering=bq1_include_volume,
            random_state=42,
        )

    if run_bq2:
        print("RUNNING BQ2")
        run_bq2(
            con=con,
            year=year,
            zone_types=bq2_zone_types,
            dist_floor_km=bq2_dist_floor_km,
            dist_power=bq2_dist_power,
        )

    print("\n GOLD finished")



if __name__ == "__main__":
    con = duckdb.connect()

    run_gold_pipeline(
        con=con,
        year=2023,
        run_bq1=True,
        run_bq2=True,
        bq1_n_clusters=3,
        bq1_zone_types=None,          
        bq2_zone_types=["districts"], # None para ejecutar todas - para ejecución rápida pruebo con uno solo
        bq2_dist_floor_km=0.1,
        bq2_dist_power=2,
    )
