# ESQ Bronze

```mermaid




erDiagram
    %% CAPA BRONZE: Ingesta Raw
    
    %% Tabla de Catálogo (Metadatos)
    BRONZE_CATALOG {
        VARCHAR filename PK
        TIMESTAMP publication_date PK
        VARCHAR main_category
        VARCHAR study_type
        VARCHAR zone_type
        INTEGER year
        INTEGER month
        VARCHAR source_url
    }

    %% Tabla Principal de Movilidad (Particionada)
    BRONZE_TRIPS {
        VARCHAR origen
        VARCHAR destino
        VARCHAR periodo
        VARCHAR viajes
        VARCHAR viajes_km
        VARCHAR fecha
        VARCHAR zone_type PK "Partition Key"
        TIMESTAMP date PK "Partition Key (Year/Month)"
        TIMESTAMP ingestion_date
        VARCHAR source_url
    }

    %% Tablas de Geometrías (Shapefiles)
    BRONZE_DISTRICTS_INFO {
        VARCHAR id_districts PK
        VARCHAR name_districts
        GEOMETRY geometry
        GEOMETRY centroid
        VARCHAR source_url
    }

    BRONZE_MUNICIPLES_INFO {
        VARCHAR id_municiples PK
        VARCHAR name_municiples
        GEOMETRY geometry
        GEOMETRY centroid
        VARCHAR source_url
    }

    BRONZE_GAUS_INFO {
        VARCHAR id_gaus PK
        VARCHAR name_gaus
        GEOMETRY geometry
        GEOMETRY centroid
        VARCHAR source_url
    }

    %% Tablas de Relación (Bridge Tables)
    BRONZE_INE_MITMA_ZONES {
        VARCHAR seccion_ine
        VARCHAR distrito_ine
        VARCHAR municipio_ine
        VARCHAR distrito_mitma
        VARCHAR municipio_mitma
        VARCHAR gau_mitma
        TIMESTAMP ingestion_date
    }

    %% Tablas INE (Demografía y Economía)
   BRONZE_RENTA_MEDIA {
        VARCHAR ine_district PK
        VARCHAR name
        VARCHAR concept
        DOUBLE year_2021_value
        DOUBLE year_2022_value
        DOUBLE year_2023_value 
        DOUBLE year_2024_value 
        VARCHAR source_url
    }

    BRONZE_POBLACION_TOTAL {
        VARCHAR ine_section PK
        VARCHAR name
        VARCHAR concept
        DOUBLE year_2021_value
        DOUBLE year_2022_value
        DOUBLE year_2023_value
        DOUBLE year_2024_value
        VARCHAR source_url
    }
    

    