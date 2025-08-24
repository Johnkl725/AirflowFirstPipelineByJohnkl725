import pandas as pd

AREAS_PATH = "dags/data/areas.xlsx"
PARQUET_PATH = "dags/data/votos.parquet"

def limpiar_votos_por_correos(**context):
    areas_df = pd.read_excel(AREAS_PATH)
    correos_validos = areas_df['Correo electronico'].tolist()
    df_votos = pd.read_excel("dags/data/¡VOTA POR LOS PRÓXIMOS EXCELENCIA TI, OPS & DATA 2025-1!(1-1628).xlsx")

    df_votos_limpio = df_votos[df_votos['Correo electrónico'].isin(correos_validos)]
    df_votos_limpio.to_parquet(PARQUET_PATH, index=False)
    print(f"[LIMPIEZA] {len(df_votos_limpio)} votos válidos guardados en {PARQUET_PATH}")

def load_areas():
    areas_df = pd.read_excel(AREAS_PATH)
    mapeo = {
        "GCIA DE AREA SOFTWARE ENGINEERING - CIO": "CIO",
        "GERENCIA DE AREA DE TECNOLOGIA - CTO": "CTO",
        "GCIA AREA ESTRAT. FINANZAS TI, DATA Y OP": "AEF",
        "TRIBU INFRASTRUCTURE & IT OPERATION": "CTO",
        "COE DATA": "Data",
        "TRIBU END USER": "AST",
        "COE CLOUD": "CTO",
        "TRIBU DATA": "Data",
        "GERENCIA DE ÁREA OPERACIONES Y PROCESOS": "Operaciones",
        "GCIA DE AREA SOLUCIONES TRANSVERSALES TI": "AST",
        "TRIBU TARJETA, SERV Y PROD DE INVERSION": "AST",
        "COE ADVANCED ANALYTICS": "Data",
        "TRIBU DATA LAB": "Data",
        "GCIA DE AREA TRANSFORMACION DE TI": "GTR",
        "GCIA DE DIVISION DATA & ANALYTICS": "Data"
    }
    areas_df['Area/Tribu/COE'] = areas_df['Area/Tribu/COE'].replace(mapeo)
    return areas_df[['Correo electronico', 'Area/Tribu/COE']].set_index('Correo electronico')
