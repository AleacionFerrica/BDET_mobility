```mermaid

erDiagram
    %% ======================
    %% CAPA GOLD (BQ1): Typical Day Patterns
    %% ======================

    %% Clusters diarios por zonificación
    GOLD_DAY_CLUSTERS {
        DATE trip_date
        VARCHAR zone_type
        INTEGER cluster_id
        VARCHAR pattern_name
        TIMESTAMP ingestion_date
    }

    %% Tabla intermedia de acumuladores (sumas + conteo de días)
    GOLD_STAGING_ACCUMULATED {
        VARCHAR zone_type
        INTEGER cluster_id
        VARCHAR pattern_name
        INTEGER hour_of_day
        INTEGER id_origin
        INTEGER id_destination
        VARCHAR distance_group_km
        DOUBLE sum_daily_trips
        DOUBLE sum_daily_length_km
        INTEGER days_count
    }

    %% Producto final: perfil horario promedio por patrón
    GOLD_TYPICAL_DAY_PATTERNS {
        VARCHAR zone_type
        INTEGER cluster_id
        VARCHAR pattern_name
        INTEGER hour_of_day
        INTEGER id_origin
        INTEGER id_destination
        VARCHAR distance_group_km
        DOUBLE avg_trips_per_day
        DOUBLE total_trips_in_cluster_sample
        INTEGER n_days_in_cluster
        TIMESTAMP ingestion_date
    }

   