import pandas as pd
from datetime import datetime
from .utils import load_areas

PARQUET_PATH = "dags/data/votos.parquet"
TOP_PARQUET_PATH = "dags/data/top_nominados.parquet"

def transform_parquet(**context):
    df = pd.read_parquet(PARQUET_PATH)
    df.columns = df.columns.str.strip()
    areas_df = load_areas()

    # --- Paso 1: Derretir votos ---
    df_melted = pd.melt(
        df,
        id_vars=['ID', 'Hora de inicio', 'Hora de finalización', 
                 'Correo electrónico', 'Nombre', 'Hora de la última modificación'],
        value_vars=['Nominados – AEF', 'Nominados - AST', 'Nominados – CTO',
                    'Nominados – CIO', 'Nominados – Operaciones', 'Nominados – Data'],
        var_name='categoria',
        value_name='persona'
    )

    # --- Paso 2: Normalizar categorías ---
    mapeo_categorias = {"aef":"aef","ast":"ast","cto":"cto",
                        "cio":"cio","operaciones":"operaciones","data":"data"}
    df_melted['nominado_area'] = (
        df_melted['categoria']
        .str.replace("–", "-", regex=False)
        .str.split("-")
        .str[-1]
        .str.strip()
        .str.lower()
        .map(mapeo_categorias)
    )

    # --- Paso 3: Área del votante ---
    df_melted['votante_area'] = df_melted['Correo electrónico'].map(
        lambda c: str(areas_df.loc[c, 'Area/Tribu/COE']).strip().lower()
        if c in areas_df.index else None
    )

    # --- Paso 4: Asignar peso ---
    def asignar_peso(row):
        if pd.isna(row['nominado_area']) or pd.isna(row['votante_area']):
            return 0
        return 1 if row['votante_area'] == row['nominado_area'] else 2

    df_melted['peso'] = df_melted.apply(asignar_peso, axis=1)

    # --- Paso 5: Crear columna es_misma_area ✅ ---
    df_melted['es_misma_area'] = df_melted['votante_area'] == df_melted['nominado_area']

    # --- Paso 6: Corrección doble conteo ---
    df_misma_area = df_melted[df_melted['es_misma_area']] \
        .drop_duplicates(subset=['Correo electrónico', 'votante_area'])
    df_otra_area = df_melted[~df_melted['es_misma_area']]
    df_final = pd.concat([df_misma_area, df_otra_area], ignore_index=True)

    # --- Paso 7: Normalizar etiquetas ---
    df_final['categoria'] = (
        df_final['categoria']
        .str.replace("–", "-", regex=False)
        .str.replace("-", "", regex=False)
        .str.replace(" ", "", regex=False)
    )

    # --- Paso 8: Resumen ---
    result = df_final.groupby(['categoria', 'persona']).agg({'peso': 'sum'}).reset_index()

    # --- Paso 9: Top 5 ---
    top_nominados = result.groupby('categoria').apply(lambda x: x.nlargest(5, 'peso')).reset_index(drop=True)

    top_nominados.insert(0, 'id', range(1, len(top_nominados) + 1))
    top_nominados['Fecha_Carga'] = datetime.now()
    top_nominados['Proceso'] = 'ETL_Top_Votados'

    top_nominados.to_parquet(TOP_PARQUET_PATH, index=False)
    print(f"[TRANSFORM] Guardado top nominados en {TOP_PARQUET_PATH}, filas: {len(top_nominados)}")