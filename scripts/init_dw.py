"""
init_dw.py
Se ejecuta una sola vez, como parte de 'airflow-init', antes de levantar
webserver/scheduler. Hace lo que docker-entrypoint-initdb.d/ hacía para un
Postgres local, pero contra un servidor PostgreSQL EXTERNO compartido:

1. Se conecta al servidor remoto usando la base de mantenimiento 'postgres'.
2. Crea la base de datos del DW si todavía no existe (nombre único para
   evitar choques con otros proyectos en el mismo servidor compartido).
3. Se conecta ya a esa base y ejecuta el script DDL completo
   (config/init_dw_schema.sql) para crear tablas, índices y vistas.

Es idempotente: si la base y las tablas ya existen, no falla ni duplica nada
(usa CREATE TABLE IF NOT EXISTS / CREATE OR REPLACE VIEW).
"""

import os
import sys
import time
import psycopg2

DW_HOST = os.environ["DW_HOST"]
DW_PORT = os.environ["DW_PORT"]
DW_DB_USER = os.environ["DW_DB_USER"]
DW_DB_PASSWORD = os.environ["DW_DB_PASSWORD"]
DW_DB_NAME = os.environ["DW_DB_NAME"]

DDL_PATH = "/opt/airflow/config/init_dw_schema.sql"

MAX_RETRIES = 5
RETRY_DELAY_SECONDS = 5


def conectar(dbname, autocommit=False):
    conn = psycopg2.connect(
        host=DW_HOST,
        port=DW_PORT,
        user=DW_DB_USER,
        password=DW_DB_PASSWORD,
        dbname=dbname,
        connect_timeout=10,
    )
    conn.autocommit = autocommit
    return conn


def esperar_servidor():
    """El servidor es externo y compartido: puede tardar en responder
    o estar momentáneamente ocupado. Reintentamos antes de fallar."""
    for intento in range(1, MAX_RETRIES + 1):
        try:
            conn = conectar("postgres", autocommit=True)
            conn.close()
            print(f"✅ Servidor remoto disponible (intento {intento}).")
            return
        except psycopg2.OperationalError as e:
            print(f"⏳ Intento {intento}/{MAX_RETRIES} — servidor no disponible aún: {e}")
            time.sleep(RETRY_DELAY_SECONDS)
    print("❌ No se pudo conectar al servidor remoto tras varios intentos.")
    sys.exit(1)


def crear_base_si_no_existe():
    conn = conectar("postgres", autocommit=True)
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (DW_DB_NAME,))
    existe = cur.fetchone()
    if existe:
        print(f"ℹ️  La base '{DW_DB_NAME}' ya existe, no se crea de nuevo.")
    else:
        # CREATE DATABASE no puede ir dentro de una transacción normal;
        # por eso esta conexión está en autocommit.
        cur.execute(f'CREATE DATABASE "{DW_DB_NAME}"')
        print(f"✅ Base de datos '{DW_DB_NAME}' creada.")
    cur.close()
    conn.close()


def aplicar_ddl():
    with open(DDL_PATH, "r", encoding="utf-8") as f:
        ddl_sql = f.read()

    conn = conectar(DW_DB_NAME, autocommit=True)
    cur = conn.cursor()
    cur.execute(ddl_sql)
    cur.close()
    conn.close()
    print(f"✅ DDL aplicado correctamente sobre '{DW_DB_NAME}'.")


if __name__ == "__main__":
    esperar_servidor()
    crear_base_si_no_existe()
    aplicar_ddl()
