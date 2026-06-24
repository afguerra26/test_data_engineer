import os
import kaggle
import pandas as pd

DATA_DIR = "/opt/airflow/data"
DATASET_1 = "carrie1/ecommerce-data"
DATASET_2 = "lakshmi25npathi/online-retail-dataset"

def descargar(dataset, subdir):
    path = os.path.join(DATA_DIR, subdir)
    os.makedirs(path, exist_ok=True)
    kaggle.api.authenticate()
    print(f"\n⬇️  Descargando {dataset}...")
    kaggle.api.dataset_download_files(dataset, path=path, unzip=True)
    return path

def perfilar(path):
    # Buscar CSV o Excel
    archivo = None
    for f in os.listdir(path):
        if f.endswith(".csv") or f.endswith(".xlsx"):
            archivo = os.path.join(path, f)
            break

    if not archivo:
        print(f"❌ No se encontró archivo en {path}")
        return

    print(f"📄 Archivo: {archivo}")
    if archivo.endswith(".xlsx"):
        df = pd.read_excel(archivo, sheet_name=0, engine="openpyxl")
    else:
        df = pd.read_csv(archivo, encoding="latin1")

    print(f"📐 Shape: {df.shape[0]:,} filas × {df.shape[1]} columnas")
    print(f"🔎 Columnas: {list(df.columns)}")
    print(f"🔎 Tipos:\n{df.dtypes}")
    print(f"🕳️  Nulos (%):\n{(df.isnull().sum() / len(df) * 100).round(2)}")
    date_col = [c for c in df.columns if 'date' in c.lower() or 'Date' in c]
    if date_col:
        print(f"⚠️  Muestra {date_col[0]}: {df[date_col[0]].iloc[:3].tolist()}")
    print(f"📋 Primeras 3 filas:\n{df.head(3)}")

if __name__ == "__main__":
    perfilar(descargar(DATASET_1, "ds1"))
    perfilar(descargar(DATASET_2, "ds2"))