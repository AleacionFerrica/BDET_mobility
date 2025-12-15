# ESQ Bronze

```mermaid




erDiagram
  
  %% TABLAS (Bronze)

  BRONZE_CATALOG {
    VARCHAR main_category
    VARCHAR study_type
    VARCHAR zone_type
    DOUBLE year
    DOUBLE month
    DOUBLE day
    TIMESTAMP_NS publication_date
    VARCHAR filename
    VARCHAR source_url
  }

  BRONZE_TRIPS {
    VARCHAR fecha
    VARCHAR periodo
    VARCHAR origen
    VARCHAR destino
    VARCHAR distancia
    VARCHAR actividad_origen
    VARCHAR actividad_destino
    VARCHAR estudio_origen_posible
    VARCHAR estudio_destino_posible
    VARCHAR residencia
    VARCHAR renta
    VARCHAR edad
    VARCHAR sexo
    VARCHAR viajes
    VARCHAR viajes_km
    VARCHAR filename
    VARCHAR zone_type
    TIMESTAMP_WITH_TIME_ZONE ingestion_date
  }

  BRONZE_DISTRICTS_INFO {
    VARCHAR id_districts
    VARCHAR name_districts
    VARCHAR zone_type
    VARCHAR source
    VARCHAR source_url
    TIMESTAMP_WITH_TIME_ZONE ingestion_date
    VARCHAR publication_date
    GEOMETRY geometry
    GEOMETRY centroid
  }

  BRONZE_GAUS_INFO {
    VARCHAR id_gaus
    VARCHAR name_gaus
    VARCHAR zone_type
    VARCHAR source
    VARCHAR source_url
    TIMESTAMP_WITH_TIME_ZONE ingestion_date
    VARCHAR publication_date
    GEOMETRY geometry
    GEOMETRY centroid
  }

  BRONZE_MUNICIPLES_INFO {
    VARCHAR id_municiples
    VARCHAR name_municiples
    VARCHAR zone_type
    VARCHAR source
    VARCHAR source_url
    TIMESTAMP_WITH_TIME_ZONE ingestion_date
    VARCHAR publication_date
    GEOMETRY geometry
    GEOMETRY centroid
  }

  BRONZE_INE_MITMA_ZONES {
    VARCHAR seccion_ine
    VARCHAR distrito_ine
    VARCHAR municipio_ine
    VARCHAR distrito_mitma
    VARCHAR municipio_mitma
    VARCHAR gau_mitma
    VARCHAR source_url
    TIMESTAMP_WITH_TIME_ZONE ingestion_date
    VARCHAR publication_date
  }

  BRONZE_RENTA_MEDIA {
    VARCHAR ine_district
    VARCHAR name
    VARCHAR concept
    VARCHAR source_url
    DOUBLE y2021_value
    DOUBLE y2022_value
    DOUBLE y2023_value
    DOUBLE y2024_value
    TIMESTAMP_WITH_TIME_ZONE ingestion_date
  }

  BRONZE_POBLACION_TOTAL {
    VARCHAR ine_section
    VARCHAR name
    VARCHAR concept
    VARCHAR source_url
    DOUBLE y2021_value
    DOUBLE y2022_value
    DOUBLE y2023_value
    DOUBLE y2024_value
    TIMESTAMP_WITH_TIME_ZONE ingestion_date
  }

  
  %% RELACIONES (lógicas)

  %% El catálogo gobierna qué ficheros se cargan en trips y en las tablas de zonificación
  BRONZE_CATALOG ||--o{ BRONZE_TRIPS : "source_url = filename"

  BRONZE_CATALOG ||--o{ BRONZE_DISTRICTS_INFO : "source_url"
  BRONZE_CATALOG ||--o{ BRONZE_GAUS_INFO : "source_url"
  BRONZE_CATALOG ||--o{ BRONZE_MUNICIPLES_INFO : "source_url"

  BRONZE_CATALOG ||--o{ BRONZE_INE_MITMA_ZONES : "source_url"

  %% Mapeo INE <-> MITMA para cruzar indicadores INE con zonas MITMA
  BRONZE_INE_MITMA_ZONES ||--o{ BRONZE_POBLACION_TOTAL : "seccion_ine = ine_section"
  BRONZE_INE_MITMA_ZONES ||--o{ BRONZE_RENTA_MEDIA : "distrito_ine = ine_district"
