import os
import json
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import kaggle

DATA_DIR  = "/opt/airflow/data"
DS1_PATH  = os.path.join(DATA_DIR, "ds1", "data.csv")
DS2_PATH  = os.path.join(DATA_DIR, "ds2", "online_retail_II.xlsx")
FUENTE_1  = "kaggle_carrie1_ecommerce"
FUENTE_2  = "kaggle_lakshmi_online_retail_II"
BATCH     = 5000

DW_CONN = dict(
    host="15.204.173.204", port=6432,
    dbname="datamart_andres_dw",
    user="coder-ra-c6",
    password=None
)

DEFAULT_ARGS = {
    "owner": "data_engineer",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
}

def get_conn():
    DW_CONN["password"] = Variable.get("dw_password")
    return psycopg2.connect(**DW_CONN)

def extract_ds1(**ctx):
    os.makedirs(os.path.join(DATA_DIR, "ds1"), exist_ok=True)
    kaggle.api.authenticate()
    print("Descargando carrie1/ecommerce-data...")
    kaggle.api.dataset_download_files("carrie1/ecommerce-data", path=os.path.join(DATA_DIR, "ds1"), unzip=True)
    print(f"Listo: {DS1_PATH}")

def extract_ds2(**ctx):
    os.makedirs(os.path.join(DATA_DIR, "ds2"), exist_ok=True)
    kaggle.api.authenticate()
    print("Descargando lakshmi25npathi/online-retail-dataset...")
    kaggle.api.dataset_download_files("lakshmi25npathi/online-retail-dataset", path=os.path.join(DATA_DIR, "ds2"), unzip=True)
    print(f"Listo: {DS2_PATH}")

def transform(**ctx):
    fx_rate = float(Variable.get("dw_fx_rate_usd", default_var=1.20))
    rechazos = []

    df1 = pd.read_csv(DS1_PATH, encoding="latin1")
    df1 = df1.rename(columns={
        "InvoiceNo": "invoice_no", "StockCode": "stock_code",
        "Description": "description", "Quantity": "quantity",
        "InvoiceDate": "invoice_date", "UnitPrice": "unit_price",
        "CustomerID": "customer_id", "Country": "country"
    })
    df1["fuente"] = FUENTE_1
    df1["invoice_date"] = pd.to_datetime(df1["invoice_date"], format="mixed", dayfirst=True, utc=True)

    df2 = pd.read_excel(DS2_PATH, sheet_name=0, engine="openpyxl")
    df2 = df2.rename(columns={
        "Invoice": "invoice_no", "StockCode": "stock_code",
        "Description": "description", "Quantity": "quantity",
        "InvoiceDate": "invoice_date", "Price": "unit_price",
        "Customer ID": "customer_id", "Country": "country"
    })
    df2["fuente"] = FUENTE_2
    df2["invoice_date"] = pd.to_datetime(df2["invoice_date"], utc=True)

    df = pd.concat([df1, df2], ignore_index=True)
    print(f"Total unificado: {len(df):,} filas")

    df["stock_code"]  = df["stock_code"].astype(str).str.strip().str.upper()
    df["description"] = df["description"].astype(str).str.strip().str.upper()
    df["invoice_no"]  = df["invoice_no"].astype(str).str.strip()
    df["clave_dedup"] = df["invoice_no"] + "_" + df["stock_code"]

    mask_interno = ~df["stock_code"].str.match(r"^\d+[A-Z]?$")
    rechazos.append(df[mask_interno].copy().assign(motivo="stockcode_interno"))
    df = df[~mask_interno].copy()

    mask_precio = df["unit_price"] <= 0
    rechazos.append(df[mask_precio].copy().assign(motivo="precio_invalido"))
    df = df[~mask_precio].copy()

    mask_cliente = df["customer_id"].isna()
    rechazos.append(df[mask_cliente].copy().assign(motivo="sin_customer_id"))
    df = df[~mask_cliente].copy()

    df["customer_id"] = df["customer_id"].astype(int).astype(str)

    mask_devol   = df["quantity"] < 0
    devoluciones = df[mask_devol].copy()
    ventas       = df[~mask_devol].copy()

    ventas["revenue_bruto"]       = (ventas["quantity"]       * ventas["unit_price"]       * fx_rate).round(4)
    devoluciones["revenue_bruto"] = (devoluciones["quantity"] * devoluciones["unit_price"] * fx_rate).round(4)

    dim = (
        ventas.groupby("stock_code")["description"]
        .agg(lambda x: x.value_counts().index[0])
        .reset_index()
        .rename(columns={"description": "nombre_canonico"})
    )
    dim["categoria"]        = "sin_categoria"
    dim["pais_origen"]      = Variable.get("dw_default_country", default_var="United Kingdom")
    dim["activo"]           = True
    dim["fuente_categoria"] = "etl_ecommerce"

    log = pd.concat(rechazos, ignore_index=True) if rechazos else pd.DataFrame()

    print(f"Ventas: {len(ventas):,} | Devoluciones: {len(devoluciones):,} | Rechazos: {len(log):,} | Productos: {len(dim):,}")

    ventas.to_csv(os.path.join(DATA_DIR, "ventas.csv"), index=False)
    devoluciones.to_csv(os.path.join(DATA_DIR, "devoluciones.csv"), index=False)
    log.to_csv(os.path.join(DATA_DIR, "rechazos.csv"), index=False)
    dim.to_csv(os.path.join(DATA_DIR, "dim_producto.csv"), index=False)

def load_dim_producto(**ctx):
    df = pd.read_csv(os.path.join(DATA_DIR, "dim_producto.csv"))
    rows = [(r["stock_code"], r["nombre_canonico"], r["categoria"],
             r["pais_origen"], bool(r["activo"]), r["fuente_categoria"])
            for _, r in df.iterrows()]
    conn = get_conn()
    try:
        cur = conn.cursor()
        execute_values(cur, """
            INSERT INTO dim_producto (codigo_producto, nombre_canonico, categoria, pais_origen, activo, fuente_categoria)
            VALUES %s
            ON CONFLICT (codigo_producto) DO UPDATE SET
                nombre_canonico     = EXCLUDED.nombre_canonico,
                fuente_categoria    = EXCLUDED.fuente_categoria,
                fecha_actualizacion = now()
        """, rows)
        conn.commit()
    finally:
        conn.close()
    print(f"dim_producto: {len(rows):,} filas.")

def load_fact_ventas(**ctx):
    df = pd.read_csv(os.path.join(DATA_DIR, "ventas.csv"))
    total = len(df)
    cargadas = 0
    for i in range(0, total, BATCH):
        lote = df.iloc[i:i+BATCH]
        rows = [(r["invoice_no"], r["stock_code"], str(r["customer_id"]),
                 r["country"], str(r["invoice_date"]),
                 int(r["quantity"]), float(r["unit_price"]),
                 float(r["revenue_bruto"]), r["fuente"], r["clave_dedup"])
                for _, r in lote.iterrows()]
        conn = get_conn()
        try:
            cur = conn.cursor()
            execute_values(cur, """
                INSERT INTO fact_ventas
                    (invoice_no, codigo_producto, customer_id, pais, fecha,
                     cantidad, precio_unitario, revenue_bruto, fuente_dataset, clave_dedup)
                VALUES %s
                ON CONFLICT (clave_dedup) DO NOTHING
            """, rows)
            conn.commit()
            cargadas += len(rows)
            print(f"Lote {i//BATCH + 1}: {cargadas:,}/{total:,} filas")
        finally:
            conn.close()
    print(f"fact_ventas: {cargadas:,} filas totales.")

def load_fact_devoluciones(**ctx):
    df = pd.read_csv(os.path.join(DATA_DIR, "devoluciones.csv"))
    total = len(df)
    cargadas = 0
    for i in range(0, total, BATCH):
        lote = df.iloc[i:i+BATCH]
        rows = [(r["invoice_no"], r["stock_code"], str(r["customer_id"]),
                 r["country"], str(r["invoice_date"]),
                 int(r["quantity"]), float(r["unit_price"]),
                 float(r["revenue_bruto"]), r["fuente"], r["clave_dedup"])
                for _, r in lote.iterrows()]
        conn = get_conn()
        try:
            cur = conn.cursor()
            execute_values(cur, """
                INSERT INTO fact_devoluciones
                    (invoice_no, codigo_producto, customer_id, pais, fecha,
                     cantidad, precio_unitario, monto, fuente_dataset, clave_dedup)
                VALUES %s
                ON CONFLICT (clave_dedup) DO NOTHING
            """, rows)
            conn.commit()
            cargadas += len(rows)
        finally:
            conn.close()
    print(f"fact_devoluciones: {cargadas:,} filas totales.")

def load_log_rechazos(**ctx):
    df = pd.read_csv(os.path.join(DATA_DIR, "rechazos.csv"))
    total = len(df)
    cargadas = 0
    for i in range(0, total, BATCH):
        lote = df.iloc[i:i+BATCH]
        rows = [(r.get("fuente", "desconocido"), r.get("motivo", "desconocido"),
                 str(r.get("invoice_no", "")), str(r.get("stock_code", "")),
                 json.dumps({
                     "description": str(r.get("description", "")),
                     "quantity":    str(r.get("quantity", "")),
                     "unit_price":  str(r.get("unit_price", "")),
                     "customer_id": str(r.get("customer_id", "")),
                 }))
                for _, r in lote.iterrows()]
        conn = get_conn()
        try:
            cur = conn.cursor()
            execute_values(cur, """
                INSERT INTO log_rechazos (fuente_dataset, motivo, invoice_no, codigo_producto, datos_originales)
                VALUES %s
            """, rows)
            conn.commit()
            cargadas += len(rows)
        finally:
            conn.close()
    print(f"log_rechazos: {cargadas:,} filas totales.")

def notify(**ctx):
    ventas   = pd.read_csv(os.path.join(DATA_DIR, "ventas.csv"))
    devol    = pd.read_csv(os.path.join(DATA_DIR, "devoluciones.csv"))
    rechazos = pd.read_csv(os.path.join(DATA_DIR, "rechazos.csv"))
    print("=" * 55)
    print("ETL completado exitosamente — dos fuentes procesadas")
    print(f"  Ventas cargadas      : {len(ventas):,}")
    print(f"  Devoluciones         : {len(devol):,}")
    print(f"  Registros rechazados : {len(rechazos):,}")
    print("=" * 55)

with DAG(
    dag_id="etl_ecommerce",
    default_args=DEFAULT_ARGS,
    description="ETL dos fuentes Kaggle -> DW remoto",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "ecommerce"],
) as dag:

    t_ext1   = PythonOperator(task_id="extract_ds1",            python_callable=extract_ds1)
    t_ext2   = PythonOperator(task_id="extract_ds2",            python_callable=extract_ds2)
    t_trans  = PythonOperator(task_id="transform",              python_callable=transform)
    t_dim    = PythonOperator(task_id="load_dim_producto",      python_callable=load_dim_producto)
    t_vtas   = PythonOperator(task_id="load_fact_ventas",       python_callable=load_fact_ventas)
    t_devol  = PythonOperator(task_id="load_fact_devoluciones", python_callable=load_fact_devoluciones)
    t_rech   = PythonOperator(task_id="load_log_rechazos",      python_callable=load_log_rechazos)
    t_notify = PythonOperator(task_id="notify",                 python_callable=notify)

    [t_ext1, t_ext2] >> t_trans >> t_dim >> t_vtas >> t_devol >> t_rech >> t_notify