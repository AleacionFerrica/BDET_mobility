# ESQ Bronze

```mermaid


erDiagram
    %% BRONZE LAYER %%
    BRONZE_CATALOG {
        VARCHAR filename
        TIMESTAMP publication_date
        VARCHAR main_category
        VARCHAR study_type
        VARCHAR zone_type
        INTEGER year
        INTEGER month
        VARCHAR source_url
    }
    BRONZE_TRIPS {
        VARCHAR origen
        VARCHAR destino
        VARCHAR periodo
        VARCHAR viajes
        VARCHAR viajes_km
        VARCHAR fecha
        VARCHAR zone_type
        TIMESTAMP date
        TIMESTAMP ingestion_date
        VARCHAR source_url
    }
    BRONZE_DISTRICTS_INFO {
        VARCHAR id_districts
        VARCHAR name_districts
        GEOMETRY geometry
        GEOMETRY centroid
        VARCHAR source_url
    }
    BRONZE_MUNICIPLES_INFO {
        VARCHAR id_municiples
        VARCHAR name_municiples
        GEOMETRY geometry
        GEOMETRY centroid
        VARCHAR source_url
    }
    BRONZE_GAUS_INFO {
        VARCHAR id_gaus
        VARCHAR name_gaus
        GEOMETRY geometry
        GEOMETRY centroid
        VARCHAR source_url
    }
    BRONZE_INE_MITMA_ZONES {
        VARCHAR seccion_ine
        VARCHAR distrito_ine
        VARCHAR municipio_ine
        VARCHAR distrito_mitma
        VARCHAR municipio_mitma
        VARCHAR gau_mitma
        TIMESTAMP ingestion_date
    }
    BRONZE_RENTA_MEDIA {
        VARCHAR ine_district
        VARCHAR name
        VARCHAR concept
        DOUBLE year_2021_value
        DOUBLE year_2022_value
        DOUBLE year_2023_value
        DOUBLE year_2024_value
        VARCHAR source_url
    }
    BRONZE_POBLACION_TOTAL {
        VARCHAR ine_section
        VARCHAR name
        VARCHAR concept
        DOUBLE year_2021_value
        DOUBLE year_2022_value
        DOUBLE year_2023_value
        DOUBLE year_2024_value
        VARCHAR source_url
    }

    BRONZE_CATALOG ||--|{ BRONZE_TRIPS : ""
    BRONZE_CATALOG ||--|{ BRONZE_DISTRICTS_INFO : ""
    BRONZE_CATALOG ||--|{ BRONZE_MUNICIPLES_INFO : ""
    BRONZE_CATALOG ||--|{ BRONZE_GAUS_INFO : ""
    BRONZE_CATALOG ||--|{ BRONZE_INE_MITMA_ZONES : ""
    BRONZE_INE_MITMA_ZONES ||--|{ BRONZE_RENTA_MEDIA : ""
    BRONZE_INE_MITMA_ZONES ||--|{ BRONZE_POBLACION_TOTAL : ""
    BRONZE_INE_MITMA_ZONES ||--|{ BRONZE_GAUS_INFO : ""
    BRONZE_INE_MITMA_ZONES ||--|{ BRONZE_MUNICIPLES_INFO : ""
    BRONZE_INE_MITMA_ZONES ||--|{ BRONZE_DISTRICTS_INFO : ""