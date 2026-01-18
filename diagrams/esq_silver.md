# ESQ Silver

```mermaid

erDiagram
    SILVER_OD_TRIPS {
        VARCHAR zone_type
        TIMESTAMP date
        VARCHAR id_origin
        VARCHAR id_destination
        VARCHAR distance_group_km
        DOUBLE n_trips
        DOUBLE trips_total_length_km
        BOOLEAN origin_activity_std
        BOOLEAN destination_activity_std
        TIMESTAMP ingestion_date
    }

    SILVER_DIM_ZONES {
        INTEGER id_zone
        VARCHAR original_id
        VARCHAR zone_type
        VARCHAR source
        TIMESTAMP ingestion_date
    }

    SILVER_INE_MITMA_ZONES {
        INTEGER id_sections_ine
        INTEGER id_districts_ine
        INTEGER id_municiples_ine
        INTEGER id_districts_mitma
        INTEGER id_municiples_mitma
        INTEGER id_gaus_mitma
        TIMESTAMP ingestion_date
    }

    SILVER_ZONES_INFO {
        INTEGER id_zone
        VARCHAR name_zone
        VARCHAR zone_type
        GEOMETRY geometry
        GEOMETRY centroid
        GEOMETRY visual_point
        TIMESTAMP ingestion_date
    }

    SILVER_SPAIN_POPULATION {
        INTEGER id_zone
        VARCHAR name_zone
        VARCHAR zone_type
        VARCHAR source
        INTEGER year
        DOUBLE population_raw
        DOUBLE population
        BOOLEAN is_imputed
        DOUBLE pct_change
        DOUBLE z_score_size
        BOOLEAN is_atypical
    }

    SILVER_AVERAGE_INCOME {
        INTEGER id_zone
        VARCHAR name_zone
        VARCHAR zone_type
        VARCHAR source
        INTEGER year
        DOUBLE income_raw
        DOUBLE income
        BOOLEAN is_imputed
        DOUBLE pct_change
        DOUBLE z_score_size
        BOOLEAN is_atypical
    }

    SILVER_ZONE_PAIRS {
        VARCHAR zone_type
        INTEGER id_origin
        INTEGER id_destination
        DOUBLE distance_km
    }

    SILVER_DIM_ZONES ||--|{ SILVER_OD_TRIPS : ""
    SILVER_DIM_ZONES ||--|{ SILVER_INE_MITMA_ZONES : ""
    SILVER_DIM_ZONES ||--|| SILVER_ZONES_INFO : ""
    SILVER_DIM_ZONES ||--|{ SILVER_SPAIN_POPULATION : ""
    SILVER_DIM_ZONES ||--|{ SILVER_AVERAGE_INCOME : ""
    SILVER_ZONES_INFO ||--|{ SILVER_ZONE_PAIRS : ""