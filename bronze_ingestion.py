import duckdb
import pandas as pd
import requests
import json
from datetime import datetime, timedelta
import os
from pathlib import Path
import xml.etree.ElementTree as ET
import re


# request and parse rss from mitma
def parse_datacatalog_mitma():
    catalog_URL = "https://movilidad-opendata.mitma.es/RSS.xml"
    response = requests.get(catalog_URL)
    response.raise_for_status()

    root = ET.fromstring(response.content)

    items = []
    for item in root.findall('./channel/item'):
        title = item.find('title').text.strip() if item.find('title') is not None else ""
        link = item.find('link').text.strip() if item.find('link') is not None else ""
        pub_date_raw = item.find('pubDate').text.strip() if item.find('pubDate') is not None else ""
        
        # Convertir fecha de publicación
        try:
            pub_date = datetime.strptime(pub_date_raw, "%a, %d %b %Y %H:%M:%S %Z")
        except ValueError:
            pub_date = None

        filename = link.split('/')[-1]
        lower_link = link.lower()
        lower_filename = filename.lower()

        # --- Lógica de Extracción de Metadatos ---

        # A) Categoría Principal (estudios_completos, estudios_basicos, etc.)
        main_category = "Otros"
        if "estudios_completos" in lower_link:
            main_category = "Estudios Completos"
        elif "estudios_basicos" in lower_link:
            main_category = "Estudios Basicos"
        elif "estudios_rutas" in lower_link:
            main_category = "Estudios de Rutas"
        elif "zonificacion" in lower_link:
            main_category = "Zonificacion"

        # B) Tipo de Estudio (viajes, etapas, etc.)
        study_type = "Desconocido"
        if "viajes" in lower_filename:
            study_type = "Viajes"
        elif "etapas" in lower_filename:
            if "_c" in lower_filename or "carretera" in lower_link:
                study_type = "Etapas (Carretera)"
            else:
                study_type = "Etapas"
        elif "pernoctaciones" in lower_filename:
            study_type = "Pernoctaciones"
        elif "personas" in lower_filename:
            study_type = "Personas"
        elif "frecuencia" in lower_link or "frecuencia" in lower_filename:
            study_type = "Frecuencia"
        elif "calidad" in lower_link or "descartados" in lower_filename:
            study_type = "Calidad"
        elif "od_rutas" in lower_filename:
            study_type = "Matriz OD Rutas"
        elif "relaciones_tramos" in lower_filename:
            study_type = "Relaciones Tramos-Rutas"
        elif "tramos_info" in lower_filename:
            study_type = "Info Tramos OD"
        elif "zonificacion" in lower_link:
            study_type = "Geometria/Zonificacion"
        elif "agregados" in lower_filename:
            study_type = "Datos Agregados"

        # C) Zona (municipios, distritos, GAU)
        zone_type = "N/A" # Por defecto
        if "municipios" in lower_link or "municipios" in lower_filename:
            zone_type = "Municipios"
        elif "distritos" in lower_link or "distritos" in lower_filename:
            zone_type = "Distritos"
        elif "gau" in lower_link or "gau" in lower_filename:
            zone_type = "GAU"
        elif "rutas" in lower_link:
            zone_type = "Rutas"
        
        # D) Extracción de Fecha (YYYY, MM, DD) desde el nombre del archivo
        # Buscamos patrones como 20240101 (diario) o 202401 (mensual)
        year, month, day = None, None, None
        
        # Regex para YYYYMMDD
        date_match_daily = re.search(r'(\d{4})(\d{2})(\d{2})', filename)
        # Regex para YYYYMM (archivos mensuales o tar)
        date_match_monthly = re.search(r'(\d{4})(\d{2})', filename)
        
        if date_match_daily:
            year = int(date_match_daily.group(1))
            month = int(date_match_daily.group(2))
            day = int(date_match_daily.group(3))
        elif date_match_monthly:
            year = int(date_match_monthly.group(1))
            month = int(date_match_monthly.group(2))
            # day se queda como None para mensuales

        items.append({
            "main_category": main_category, # Nueva columna solicitada
            "study_type": study_type,
            "zone_type": zone_type,
            "year": year,
            "month": month,
            "day": day,
            "publication_date": pub_date,
            "filename": filename,
            "source_url": link
        })
    df_catalog = pd.DataFrame(items)
    return df_catalog

# Funcion para ingerir datos viajes

def load_trips(con, year, zone, month=None):
    # zone format : 'Municipio' 'Distritos' 'GAUS'
    table_name = "trips"

    

    # saca urls del catalogo
    urls_query = f"""
        SELECT source_url 
        FROM bronze.catalog 
        WHERE year = {year}
        AND month = {month}
        AND zone_type = '{zone}'
        AND main_category = 'Estudios Basicos'
        AND study_type = 'Viajes'
        AND filename LIKE '%.csv.gz'
    """
    if month is None: pass # no month chosen load whole year

    elif isinstance(month, int):
        urls_query += f" AND month = {month}"
        
    elif isinstance(month, (list, tuple, range)):
        months_str = ",".join(map(str, month))
        urls_query += f" AND month IN ({months_str})"
    files_df = con.sql(urls_query).df()


    urls = files_df['source_url'].tolist()[-7:]
    if not urls:
        print("URLS not found for processing.")
        return
    urls_sql_list = str(urls).replace('[', '').replace(']', '')

    print(f"Procesing {len(urls)} files into bronze.{table_name}")
    source_query = f"""
                SELECT 
                    *,
                    '{zone}'as zone_type ,
                    current_timestamp as ingestion_date --añadir columna para controlar fecha de ingesta
                    FROM read_csv(
                            [{urls_sql_list}],
                            header=True, 
                            filename=True, -- Nos da el nombre del archivo que es la url de la pagina de mitma
                            union_by_name=True,
                            null_padding=True,
                            ignore_errors=True,
                            all_varchar=True)-- Leemos todo como texto para evitar fallos de tipo antes de castear
                            """
    # Crear tabla
    con.sql(f"""
            CREATE TABLE IF NOT EXISTS  bronze.{table_name} AS
            {source_query} LIMIT 0;
            """)

    # Delete Files if already in table 
    con.sql(f"""
        DELETE FROM bronze.{table_name} 
        WHERE filename IN ({urls_sql_list})
    """)
    # Insert into table
    con.sql(f"""
        INSERT INTO bronze.{table_name} 
        {source_query}
    """)

    con.sql(f"SELECT count(*) as rows_inserted_in_bronze_trips FROM bronze.trips").show()
    
def load_zone_info(con,zone):
    
    # zone = 'municipios','distritos','gaus'
    #Find in catalog zone type files
    zone_dic = {"municipios":"municiples","distritos":"districts","gaus":"gaus"}
    

    
    file_info = con.sql(f"""
    SELECT source_url, filename, publication_date
    FROM bronze.catalog 
    WHERE (main_category = 'Zonificacion' OR main_category = 'Otros')
    AND (filename ILIKE '%{zone}%') 
    
    AND (filename ILIKE '%.shp' OR filename ILIKE '%.shx' OR filename ILIKE '%.dbf' OR filename ILIKE '%.prj' OR filename ILIKE '%.csv')
    
        """).df()
    if file_info.empty:
        print(f"Files not found for {zone} in bronze.catalog")
        return
   
    #temp dowload data
    urls = file_info["source_url"]
    publication = file_info["publication_date"]
    download_dir = f"temp_downloads/{zone}"
    os.makedirs(download_dir, exist_ok=True)

    for key, url in urls.items():
        filename = url.split('/')[-1]
        path = f"{download_dir}/{filename}"
        # Descargamos solo si no existe para ahorrar tiempo
        if not os.path.exists(path):
            # print(f"Descargando {filename}...")
            r = requests.get(url)
            with open(path, 'wb') as f:
                f.write(r.content)




    shp_path = f"{download_dir}/zonificacion_{zone}.shp"
    shp_centroid_path = f"{download_dir}/zonificacion_{zone}_centroides.shp"
    csv_path = f"{download_dir}/nombres_{zone}.csv"
    main_source_url = urls.iloc[0]
    main_publication_date = publication.iloc[0]
    if not (os.path.exists(shp_path) and os.path.exists(csv_path)):
        print(f"Missing files (SHP o CSV) in {download_dir}. Saltando carga.")
        return
    
    #con.sql(f"DELETE FROM bronze.{zone_dic[zone]}_info")

    source_query = f"""  SELECT 

                    CAST(t1.ID AS VARCHAR) as id_{zone_dic[zone]},
                    t2.name as name_{zone_dic[zone]},
                    
                    -- Geometría (Se guardará como binario WKB automáticamente)

                    '{zone_dic[zone]}' as zone_type,
                    'MITMA' as source,
                    '{main_source_url}' as source_url,      
                    current_timestamp as ingestion_date,
                    '{main_publication_date}' as publication_date,
                    t1.geom as geometry,
                    t3.geom as centroid
                FROM st_read('{shp_path}') t1
                -- Left Join con el CSV de nombres
                LEFT JOIN read_csv('{csv_path}', delim='|', header=True, auto_detect=True) t2 
                ON CAST(t1.ID AS VARCHAR) = CAST(t2.ID AS VARCHAR)
                LEFT JOIN st_read('{shp_centroid_path}') t3 ON CAST(t1.ID AS VARCHAR) = CAST(t3.ID AS VARCHAR)
                """
    #con.sql(f"SELECT * FROM ({source_query})").show()

    # Crea tabla de info sobre las zonas
    con.sql(f"""
    CREATE TABLE IF NOT EXISTS bronze.{zone_dic[zone]}_info  AS {source_query} LIMIT 0
        """)
    

    con.sql(f"""
            INSERT INTO bronze.{zone_dic[zone]}_info

                SELECT * FROM ({source_query})  AS new_data
                WHERE NOT EXISTS (
                    SELECT 1 
                    FROM bronze.{zone_dic[zone]}_info AS existing
                    WHERE existing.source_url = new_data.source_url
                    AND existing.id_{zone_dic[zone]} = new_data.id_{zone_dic[zone]}
                )
            
        """)
    con.sql(f"SELECT count(*) as rows_inserted_into_{zone_dic[zone]}_info FROM bronze.{zone_dic[zone]}_info").show()

def load_ine_mitma_zone_relation():
    file_info = con.sql(f"""
      SELECT source_url, filename, publication_date
      FROM bronze.catalog 
      WHERE (main_category = 'Zonificacion' OR main_category = 'Otros')
        AND (filename ILIKE '%relacion%') 

          """).df()
    if file_info.empty : 
        print("'relacion' data not found in bronze.catalog")
        pass
    publication_date = file_info["publication_date"].iloc[0]
    #print(publication_date)
    urls = file_info["source_url"].tolist()
    urls_sql_list = str(urls).replace('[', '').replace(']', '')
    print(urls_sql_list)
    source_query = f"""--sql
                SELECT 
                    *,
                    filename as source_url,
                    current_timestamp as ingestion_date, --añadir columna para controlar fecha de ingesta
                    '{publication_date}' as publication_date
                    FROM read_csv(
                            [{urls_sql_list}],
                            header=True, 
                            
                            union_by_name=True,
                            null_padding=True,
                            ignore_errors=True,
                            all_varchar=True)-- Leemos todo como texto para evitar fallos de tipo antes de castear
                            """
    
    #con.sql(source_query).show()

    con.sql(f"""
            CREATE TABLE IF NOT EXISTS  bronze.ine_mitma_zones AS
            {source_query} LIMIT 0;
            """)

    con.sql(f"""
            INSERT INTO bronze.ine_mitma_zones

                SELECT * FROM ({source_query})  AS new_data
                WHERE NOT EXISTS ( 
                    SELECT 1 
                    FROM bronze.ine_mitma_zones AS existing
                    WHERE existing.seccion_ine = new_data.seccion_ine
                    AND existing.distrito_ine = new_data.distrito_ine
                    AND existing.municipio_ine = new_data.municipio_ine
                    AND existing.distrito_mitma = new_data.distrito_mitma
                    AND existing.municipio_mitma = new_data.municipio_mitma
                    AND existing.gau_mitma = new_data.gau_mitma
                )
            
        """)

def parse_rent_ine():

    TABLE_ID = "30824"  # 30824 total albacete : 30656
    URL = f"https://servicios.ine.es/wstempus/js/es/DATOS_TABLA/{TABLE_ID}?tip=AM"


    # request json file from ine 
    try:
        # request json file of the table 
        response = requests.get(URL)
        response.raise_for_status()
        resultados = []
        #print(response.text)
        # print(type(response.json()))
        for entrada in response.json():
            # Filtro: Solo Distritos
            #print(entrada)
            es_distrito = False
            codigo_distrito = "N/A"
            nombre_distrito = "N/A"
            
            #Test metadata is not empty
            metadata = entrada.get("MetaData", [])
            if metadata is None: continue
            for meta in entrada.get("MetaData", []):
                if meta.get("T3_Variable") == "Distritos":
                    es_distrito = True
                    codigo_distrito = meta.get("Codigo")
                    nombre_distrito = meta.get("Nombre")
                    break
            
            if not es_distrito:
                continue

            # Limpieza del nombre del concepto
            nombre_completo = entrada.get("Nombre", "")
            try:
                concepto = nombre_completo.split('.')[-2].strip()
            except:
                concepto = nombre_completo
            if concepto != "Renta neta media por persona":
                continue
            else:
                concepto = "renta_media"
            #print(concepto)
            # Filtro de Años
            for dato in entrada.get("Data", []):
                anyo = dato.get("Anyo")
                if 2021 <= anyo <=2024:
                    resultados.append({
                        "ine_district": codigo_distrito,
                        "name": nombre_distrito,
                        "concept": concepto,
                        "year": f"{anyo}_value",
                        "avg_net_income": dato.get("Valor"),
                        "source_url": URL
                    })

        if resultados:
            df = pd.DataFrame(resultados)

            df_pivot = df.pivot_table(
                        index=['ine_district', 'name', 'concept',"source_url"],
                        columns='year',
                        values='avg_net_income'

                    ).reset_index()
            print(f" Parsed {len(df_pivot)} rows from INE {concepto}" )
            return df_pivot
        else:
            print("Rent data not found.")
    except Exception as e:
        print(f"Error: {e}")


def parse_population_ine():

    # TABLA DE POBLACION

    TABLE_ID = "66595"  # 65031 total, albacete : 69095
    URL = f"https://servicios.ine.es/wstempus/js/es/DATOS_TABLA/{TABLE_ID}?tip=AM"

    print (f"Parsing population data from {URL}")
    # 2. Hacemos la petición
    try:
        # request json file of the table 
        response = requests.get(URL)
        response.raise_for_status()
        resultados = []
        # print(response.text)
        # print(type(response.json()))
        for entrada in response.json():
            metadata = entrada.get("MetaData", [])
            
            # --- Variables de control (Flags) ---
            es_seccion = False
            codigo_seccion = None
            nombre_seccion = None
            
            es_total_actividad = False
            es_total_pais = False
            es_total_sexo = False
            
            # --- Iterar metadatos para verificar filtros ---
            for meta in metadata:
                variable = meta.get("T3_Variable")
                nombre = meta.get("Nombre") # Ejemplo: "Total", "Ocupado/a", "Extranjero"
                
                # 1. Filtro Geográfico: Solo Secciones
                if variable == "Secciones":
                    es_seccion = True
                    codigo_seccion = meta.get("Codigo") # Ejemplo: 0100101001
                    nombre_seccion = meta.get("Nombre")

                # 2. Filtro Actividad: Debe ser "Total" (para excluir "Ocupados", "Estudiantes")
                elif variable == "Relación con la actividad" and nombre == "Total":
                    es_total_actividad = True
                    
                # 3. Filtro Nacionalidad: Debe ser "Total" (para excluir "Española", "Extranjera")
                elif variable in ["Países", "Nacionalidad"] and nombre == "Total":
                    es_total_pais = True

                # 4. Filtro Sexo: Debe ser "Total"
                elif variable == "Sexo" and nombre == "Total":
                    es_total_sexo = True

            # --- Decisión de Guardado ---
            # Solo procesamos si es una Sección Y además es el TOTAL de todas las categorías
            if es_seccion and es_total_actividad and es_total_pais and es_total_sexo:
                
                for dato in entrada.get("Data", []):
                    # Filtro de años opcional (ej. 2021-2024)
                    anyo = dato.get("Anyo")
                    if 2021 <= anyo <= 2024: 
                        resultados.append({
                            "ine_section": codigo_seccion,
                            "name": nombre_seccion,
                            "concept": "poblacion_total", # Según tu JSON es pob > 16 años
                            "year": f"{anyo}_value",
                            "total_population": dato.get("Valor"),
                            "source_url": URL
                        })

        # Convertir a DataFrame y Pivotar
        if resultados:
            df = pd.DataFrame(resultados)
            
            # Pivotamos para que los años sean columnas (formato wide)
            df_pivot = df.pivot_table(
                index=['ine_section', 'name', 'concept', 'source_url'],
                columns='year',
                values='total_population'
            ).reset_index()
            print(f" Parsed {len(df_pivot)} rows from INE Poblacion Total" )
            df_pivot.columns.name = None
            return df_pivot
        else:
            print("Population data not found.")
    except Exception as e:
        print(f"Error: {e}")

def load_ine_data(df):
    if df.empty : 
        print("INE data not found")
        pass
    table_name = df["concept"][0]
    #print(f"bronze.{table_name}")
    con.sql(f"CREATE TABLE IF NOT EXISTS bronze.{table_name} AS SELECT *,current_timestamp as ingestion_date FROM df LIMIT 0")
    for colname in df.columns.tolist():
            if colname.startswith("ine_"):
                ine_id = colname
            con.sql(f"""ALTER TABLE bronze.{table_name} ADD COLUMN IF NOT EXISTS "{colname}" VARCHAR;""") 

        # loop para meter todas las columnas del dataframe, en caso de actualizacion de datos del ine (2025) se añadiran estos datos sin error

    #con.sql(f"DESCRIBE TABLE bronze.{table_name}").show()
    con.sql(f"""
            INSERT INTO bronze.{table_name} BY NAME
            (SELECT *, 
            current_timestamp as ingestion_date
            FROM df AS new_data
            WHERE NOT EXISTS (
                SELECT 1 
                FROM bronze.{table_name} AS existing
                WHERE existing.name = new_data.name
                AND existing.{ine_id} = new_data.{ine_id}
                AND existing.concept = new_data.concept
            ))
        """)

if __name__ == "__main__":
    con = duckdb.connect()
    con.sql("""INSTALL ducklake; LOAD ducklake;
                INSTALL spatial; LOAD spatial;
    """)
    con.sql(f"""
            ATTACH 'ducklake:mobility.ducklake' AS my_ducklake;
            USE my_ducklake;
            CREATE SCHEMA IF NOT EXISTS bronze;
                """)

    # Obtenemos el catalogo de datos del MITMA
    df_catalog = parse_datacatalog_mitma() # parseamos el XML en un dataframe ya que el archivo es pequeño < 10.000 filas
    con.sql("CREATE TABLE IF NOT EXISTS bronze.catalog AS SELECT * FROM df_catalog LIMIT 0")
    # Insertamos en el catalogo solo datos que no estan ya ,ie, que no tengan la misma fecha de publicacion y nombre de archivo, asegurando no duplicados
    con.sql("""
            INSERT INTO bronze.catalog
            SELECT * FROM df_catalog AS new_data
            WHERE NOT EXISTS (
                SELECT 1 
                FROM bronze.catalog AS existing
                WHERE existing.filename = new_data.filename
                AND existing.publication_date = new_data.publication_date
            )
        """)

    # load_trips(con, year=2023, zone="Distritos", month=6)
    # load_trips(con, year=2023, zone="GAU", month=6)
    
    # # con.sql("DROP TABLE bronze.districts_info ")
    # # con.sql("DROP TABLE bronze.municiples_info ")
    # # con.sql("DROP TABLE bronze.gaus_info ")

    # # # LOAD zones info
    # load_zone_info(con,zone="distritos")
    # load_zone_info(con,zone="municipios")
    # load_zone_info(con,zone="gaus")


    # df_rent = parse_rent_ine()
    

    df_pop = parse_population_ine()
    # print(df_pop[2023].sum())
    #print(df_pop)
    
    # load_ine_data(df_rent)
    load_ine_data(df_pop)
    #print(len(con.sql("SELECT * FROM bronze.poblacion_total --WHERE ine_section LIKE '02%' ").df()))

    load_ine_mitma_zone_relation()

    #con.sql("SELECT * FROM bronze.renta_media -- WHERE seccion_ine LIKE '02%' ").show()
    









