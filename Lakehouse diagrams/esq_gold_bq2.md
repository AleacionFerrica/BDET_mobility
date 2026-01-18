```mermaid

erDiagram
    %% ======================
    %% CAPA GOLD (BQ2): Gravity Model + Infrastructure Gaps
    %% ======================

    GOLD_GRAVITY_PAIR_FEATURES {
        VARCHAR zone_type
        INTEGER year
        INTEGER id_origin
        INTEGER id_destination
        DOUBLE distance_km
        DOUBLE actual_trips
        DOUBLE pop_origin
        DOUBLE inc_destination
        DOUBLE x_ij
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
        VARCHAR zone_type
        INTEGER year
        INTEGER id_origin
        INTEGER id_destination
        DOUBLE distance_km
        DOUBLE actual_trips
        DOUBLE theoretical_trips
        DOUBLE mismatch_ratio
        DOUBLE gap
        TIMESTAMP ingestion_date
    }

   