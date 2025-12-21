```mermaid

erDiagram
  %% =========================
  %% TABLAS (Gold)
  %% =========================

  GOLD_DAY_FEATURES {
    DATE trip_date
    VARCHAR zone_type
    DOUBLE share_h00
    DOUBLE share_h01
    DOUBLE share_h02
    DOUBLE share_h03
    DOUBLE share_h04
    DOUBLE share_h05
    DOUBLE share_h06
    DOUBLE share_h07
    DOUBLE share_h08
    DOUBLE share_h09
    DOUBLE share_h10
    DOUBLE share_h11
    DOUBLE share_h12
    DOUBLE share_h13
    DOUBLE share_h14
    DOUBLE share_h15
    DOUBLE share_h16
    DOUBLE share_h17
    DOUBLE share_h18
    DOUBLE share_h19
    DOUBLE share_h20
    DOUBLE share_h21
    DOUBLE share_h22
    DOUBLE share_h23
    DOUBLE total_trips
    DOUBLE peak_hour
    DOUBLE morning_share
    DOUBLE evening_share
    INTEGER weekday
    BOOLEAN is_weekend
    TIMESTAMP ingestion_date
  }

  GOLD_DAY_CLUSTERS {
    DATE trip_date
    VARCHAR zone_type
    INTEGER cluster_id
    VARCHAR pattern_name
    TIMESTAMP ingestion_date
  }

  GOLD_TYPICAL_DAY_PATTERNS {
    VARCHAR zone_type
    INTEGER cluster_id
    VARCHAR pattern_name
    INTEGER hour_of_day
    VARCHAR id_origin
    VARCHAR id_destination
    DOUBLE avg_trips_per_day
    DOUBLE avg_trip_length_km
    DOUBLE total_trips_in_cluster_sample
    BIGINT n_days_in_cluster
    TIMESTAMP ingestion_date
  }

  GOLD_ZONE_YEAR_ATTRIBUTES {
    VARCHAR zone_type
    INTEGER year
    INTEGER id_zone
    DOUBLE population
    DOUBLE economic_activity_proxy
    TIMESTAMP ingestion_date
  }

  GOLD_GRAVITY_PARAMS {
    VARCHAR zone_type
    INTEGER year
    DOUBLE k
    BIGINT n_pairs_used
    TIMESTAMP fitted_at
  }

  GOLD_INFRASTRUCTURE_GAPS {
    INTEGER id_origin
    INTEGER id_destination
    VARCHAR zone_type
    INTEGER year
    DOUBLE distance_km
    DOUBLE actual_trips
    DOUBLE theoretical_trips
    DOUBLE mismatch_ratio
    DOUBLE gap
    TIMESTAMP ingestion_date
  }

  GOLD_ZONE_GAP_RANKING {
    VARCHAR zone_type
    INTEGER year
    INTEGER id_zone
    DOUBLE gap_outgoing
    DOUBLE gap_incoming
    DOUBLE weighted_gap
    BIGINT rank_worst_served
    TIMESTAMP ingestion_date
  }

  %% =========================
  %% TABLAS SILVER (referenciadas por Gold)
  %% =========================

  SILVER_OD_TRIPS {
    TIMESTAMP date
    VARCHAR zone_type
    VARCHAR id_origin
    VARCHAR id_destination
    DOUBLE n_trips
    DOUBLE trips_total_length_km
  }

  SILVER_ZONE_PAIRS {
    VARCHAR zone_type
    INTEGER id_origin
    INTEGER id_destination
    DOUBLE distance_km
  }

  SILVER_SPAIN_POPULATION {
    INTEGER id_zone
    INTEGER year
    DOUBLE population
  }

  SILVER_AVERAGE_RENT {
    INTEGER id_zone
    INTEGER year
    DOUBLE rent
  }

  SILVER_INE_MITMA_ZONES {
    INTEGER id_sections_ine
    INTEGER id_districts_ine
    INTEGER id_districts_mitma
    INTEGER id_municiples_mitma
    INTEGER id_gaus_mitma
  }

  %% =========================
  %% RELACIONES (Gold ↔ Gold)
  %% =========================

  GOLD_DAY_FEATURES ||--o{ GOLD_DAY_CLUSTERS : "(trip_date, zone_type)"
  GOLD_DAY_CLUSTERS ||--o{ GOLD_TYPICAL_DAY_PATTERNS : "(zone_type, cluster_id, pattern_name)"

  GOLD_ZONE_YEAR_ATTRIBUTES ||--o{ GOLD_INFRASTRUCTURE_GAPS : "id_zone = id_origin"
  GOLD_ZONE_YEAR_ATTRIBUTES ||--o{ GOLD_INFRASTRUCTURE_GAPS : "id_zone = id_destination"

  GOLD_GRAVITY_PARAMS ||--o{ GOLD_INFRASTRUCTURE_GAPS : "(zone_type, year)"
  GOLD_INFRASTRUCTURE_GAPS ||--o{ GOLD_ZONE_GAP_RANKING : "id_origin/id_destination -> id_zone (agregado)"

  %% =========================
  %% RELACIONES (Silver → Gold) (dependencias)
  %% =========================

  SILVER_OD_TRIPS ||--o{ GOLD_DAY_FEATURES : "date -> trip_date (agregado por día/hora)"
  SILVER_OD_TRIPS ||--o{ GOLD_TYPICAL_DAY_PATTERNS : "OD hourly -> patrón típico"

  SILVER_SPAIN_POPULATION ||--o{ GOLD_ZONE_YEAR_ATTRIBUTES : "(id_zone, year) -> population"
  SILVER_AVERAGE_RENT ||--o{ GOLD_ZONE_YEAR_ATTRIBUTES : "(id_zone, year) -> economic_activity_proxy"

  SILVER_INE_MITMA_ZONES ||--o{ GOLD_ZONE_YEAR_ATTRIBUTES : "mapeo INE->MITMA (target_col)"

  SILVER_ZONE_PAIRS ||--o{ GOLD_INFRASTRUCTURE_GAPS : "(zone_type, id_origin, id_destination) -> distance_km"
