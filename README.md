
# Proyecto Integrador: Pipeline ETL Orquestado con Airflow y PostgreSQL

## Enfoque Data Engineering
Este proyecto está diseñado para demostrar buenas prácticas de ingeniería de datos en la automatización, orquestación y control de calidad de pipelines ETL. Utiliza Airflow para la gestión de dependencias, escalabilidad y trazabilidad, y PostgreSQL como destino confiable para almacenamiento estructurado.

### Características Data Engineer
- **Automatización y Orquestación:** Todo el flujo ETL es gestionado por Airflow, permitiendo ejecución programada, monitoreo y reintentos automáticos.
- **Calidad y Normalización de Datos:** Se aplican filtros, limpieza, control de duplicados y normalización de columnas para asegurar consistencia y calidad.
- **Escalabilidad:** El uso de Docker y Parquet permite procesar grandes volúmenes de datos y escalar el pipeline fácilmente.
- **Auditoría y Trazabilidad:** Cada registro incluye metadatos de carga y proceso, y la tabla final tiene un campo autoincremental como clave primaria.
- **Exportación y Reproducibilidad:** El resultado puede ser exportado a CSV para análisis externo o integración con otros sistemas.


## Estructura del Proyecto
- `dags/etl_votos2.py`: DAG principal, orquestador modular.
- `etl/`: Módulos Python con funciones separadas para limpieza (`utils.py`), extracción (`extract.py`), transformación (`transform.py`) y carga (`load.py`).
- `dags/data/`: Carpeta con archivos fuente y mapeos.
- `Dockerfile`, `docker-compose.yaml`: Infraestructura reproducible y portable.
- `tabla_auditoria_votados.csv`: Exportación final para análisis o reporting.
- `README.md`: Documentación y guía de uso.


## Pipeline ETL Modular
1. **Limpieza previa:**
  - `limpiar_votos_por_correos` (etl/utils.py): Elimina o corrige registros inválidos antes de la extracción.
2. **Extracción:**
  - `extract_excel_to_parquet` (etl/extract.py): Lee el Excel y convierte a Parquet.
3. **Transformación:**
  - `transform_parquet` (etl/transform.py): Aplica reglas de negocio, asigna pesos, controla duplicados y normaliza datos.
4. **Carga:**
  - `load_top_to_postgres` (etl/load.py): Inserta los resultados en PostgreSQL, con control incremental y clave primaria.
5. **Exportación:**
  - Exporta la tabla final a CSV para análisis externo.

Cada función modular puede ser testeada y extendida de forma independiente, siguiendo buenas prácticas de ingeniería de datos.

## Comandos Data Engineer
- **Levantar el entorno:**
  ```bash
  docker-compose up -d
  ```
- **Acceso a PostgreSQL:**
  ```bash
  docker exec -it proyectointegrador-postgres-1 psql -U airflow -d airflow
  ```
- **Truncar tabla:**
  ```bash
  docker exec -i proyectointegrador-postgres-1 psql -U airflow -d airflow -c "TRUNCATE tabla_auditoria_votados;"
  ```
- **Exportar a CSV:**
  ```bash
  docker exec -i proyectointegrador-postgres-1 psql -U airflow -d airflow -c "\COPY tabla_auditoria_votados TO '/tmp/tabla_auditoria_votados.csv' WITH CSV HEADER;"
  docker cp proyectointegrador-postgres-1:/tmp/tabla_auditoria_votados.csv ./tabla_auditoria_votados.csv
  ```

## Requisitos Técnicos
- Docker
- Airflow
- PostgreSQL
- Python 3.12
- Pandas, SQLAlchemy

## Buenas Prácticas
- Modulariza los DAGs y tareas.
- Versiona los scripts y configura variables en archivos separados.
- Usa Parquet para eficiencia y portabilidad.
- Documenta cada paso y mantén logs claros.

## Personalización y Extensión
- Puedes agregar nuevos DAGs, sensores, notificaciones o integraciones con APIs.
- Modifica los archivos fuente en `dags/data/` para nuevos casos de negocio.

## Autor
Johnkl725

---
¿Buscas ejemplos avanzados, integración con Spark, o monitoreo con herramientas externas? ¡Consúltame!
