# ESQ Silver

```mermaid


erDiagram

  SILVER_DIM_ZONES {
    INTEGER id_zone
    VARCHAR original_id
    VARCHAR zone_type
    VARCHAR source
    TIMESTAMP ingestion_date
  }

  SILVER_OD_TRIPS {
    TIMESTAMP date
    VARCHAR zone_type
    VARCHAR id_origin
    VARCHAR id_destination
    VARCHAR origin_activity
    VARCHAR destination_activity
    VARCHAR distance_group_km
    VARCHAR residence_province
    VARCHAR rent_group
    VARCHAR age_group
    VARCHAR sex_group
    DOUBLE n_trips
    DOUBLE trips_total_length_km
    BOOLEAN origin_activity_std
    BOOLEAN destination_activity_std
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
    VARCHAR country_zone
    GEOMETRY geometry
    GEOMETRY centroid
    GEOMETRY visual_point
    TIMESTAMP ingestion_date
  }

  SILVER_ZONE_PAIRS {
    VARCHAR zone_type
    INTEGER id_origin
    INTEGER id_destination
    DOUBLE distance_km
  }

  SILVER_SPAIN_POPULATION {
    INTEGER id_zone
    VARCHAR name
    INTEGER year
    DOUBLE population_raw
    DOUBLE population
    BOOLEAN is_imputed
    DOUBLE pct_change
    DOUBLE z_score_size
    BOOLEAN is_atypical
  }

  SILVER_AVERAGE_RENT {
    INTEGER id_zone
    VARCHAR name
    INTEGER year
    DOUBLE rent_raw
    DOUBLE rent
    BOOLEAN is_imputed
    DOUBLE pct_change
    DOUBLE z_score_size
    BOOLEAN is_atypical
  }

  %% =========================
  %% RELACIONES (lógicas / FKs)
  %% =========================

  %% dim_zones es la dimensión central (SK = id_zone)
  SILVER_DIM_ZONES ||--o{ SILVER_OD_TRIPS : "id_zone -> id_origin"
  SILVER_DIM_ZONES ||--o{ SILVER_OD_TRIPS : "id_zone -> id_destination"

  SILVER_DIM_ZONES ||--o{ SILVER_ZONES_INFO : "id_zone"

  %% mapeo INE<->MITMA: todas las columnas id_* apuntan a dim_zones.id_zone
  SILVER_DIM_ZONES ||--o{ SILVER_INE_MITMA_ZONES : "id_zone -> id_sections_ine"
  SILVER_DIM_ZONES ||--o{ SILVER_INE_MITMA_ZONES : "id_zone -> id_districts_ine"
  SILVER_DIM_ZONES ||--o{ SILVER_INE_MITMA_ZONES : "id_zone -> id_municiples_ine"
  SILVER_DIM_ZONES ||--o{ SILVER_INE_MITMA_ZONES : "id_zone -> id_districts_mitma"
  SILVER_DIM_ZONES ||--o{ SILVER_INE_MITMA_ZONES : "id_zone -> id_municiples_mitma"
  SILVER_DIM_ZONES ||--o{ SILVER_INE_MITMA_ZONES : "id_zone -> id_gaus_mitma"

  %% pares OD (distancias) derivados de zones_info (misma semántica que OD)
  SILVER_ZONES_INFO ||--o{ SILVER_ZONE_PAIRS : "id_zone -> id_origin"
  SILVER_ZONES_INFO ||--o{ SILVER_ZONE_PAIRS : "id_zone -> id_destination"
  SILVER_ZONE_PAIRS ||--o{ SILVER_OD_TRIPS : "(zone_type,id_origin,id_destination)"

  %% indicadores socioeconómicos por zona (id_zone ya mapeado desde INE a dim_zones)
  SILVER_DIM_ZONES ||--o{ SILVER_SPAIN_POPULATION : "id_zone"
  SILVER_DIM_ZONES ||--o{ SILVER_AVERAGE_RENT : "id_zone"
