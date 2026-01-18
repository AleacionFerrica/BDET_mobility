# ESQ Silver

```mermaid


erDiagram
	direction TB
	SILVER_DIM_ZONES {
		INTEGER id_zone PK ""  
		VARCHAR original_id  ""  
		VARCHAR zone_type  ""  
		VARCHAR source  ""  
		TIMESTAMP ingestion_date  ""  
	}

	SILVER_OD_TRIPS {
		VARCHAR zone_type PK "Partition Key"  
		TIMESTAMP date PK "Partition Key (Year/Month)"  
		VARCHAR id_origin FK ""  
		VARCHAR id_destination FK ""  
		VARCHAR distance_group_km  ""  
		DOUBLE n_trips  ""  
		DOUBLE trips_total_length_km  ""  
		BOOLEAN origin_activity_std  ""  
		BOOLEAN destination_activity_std  ""  
		TIMESTAMP ingestion_date  ""  
	}

	SILVER_INE_MITMA_ZONES {
		INTEGER id_sections_ine  ""  
		INTEGER id_districts_ine  ""  
		INTEGER id_municiples_ine  ""  
		INTEGER id_districts_mitma  ""  
		INTEGER id_municiples_mitma  ""  
		INTEGER id_gaus_mitma  ""  
		TIMESTAMP ingestion_date  ""  
	}

	SILVER_ZONES_INFO {
		INTEGER id_zone PK ""  
		VARCHAR name_zone  ""  
		VARCHAR zone_type  ""  
		GEOMETRY geometry  ""  
		GEOMETRY centroid  ""  
		GEOMETRY visual_point  ""  
		TIMESTAMP ingestion_date  ""  
	}

	SILVER_ZONE_PAIRS {
		VARCHAR zone_type  ""  
		INTEGER id_origin PK ""  
		INTEGER id_destination PK ""  
		DOUBLE distance_km  ""  
	}

	SILVER_SPAIN_POPULATION {
		INTEGER id_zone PK ""  
		VARCHAR name_zone  ""  
		VARCHAR zone_type  ""  
		VARCHAR source  ""  
		INTEGER year PK ""  
		DOUBLE population_raw  ""  
		DOUBLE population  ""  
		BOOLEAN is_imputed  ""  
		DOUBLE pct_change  ""  
		DOUBLE z_score_size  ""  
		BOOLEAN is_atypical  ""  
	}

	SILVER_AVERAGE_INCOME {
		INTEGER id_zone PK ""  
		VARCHAR name_zone  ""  
		VARCHAR zone_type  ""  
		VARCHAR source  ""  
		INTEGER year PK ""  
		DOUBLE income_raw  ""  
		DOUBLE income  ""  
		BOOLEAN is_imputed  ""  
		DOUBLE pct_change  ""  
		DOUBLE z_score_size  ""  
		BOOLEAN is_atypical  ""  
	}