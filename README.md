# Pipeline ETL con Apache Airflow — DataMart S.A.S.

**Autor:** Andrés Guerra  
**Programa:** Ingeniería de Datos — RIWI · Cohorte 6 · 2026

Pipeline ETL completo que ingesta dos fuentes de datos heterogéneas de Kaggle, aplica reglas de calidad de negocio y carga los datos transformados en un repositorio analítico PostgreSQL, orquestado con Apache Airflow dentro de Docker.

---

## Stack tecnológico

- Apache Airflow 2.10.5 — orquestación del pipeline
- Docker + Docker Compose — entorno reproducible
- PostgreSQL — repositorio analítico remoto
- Python 3.12 — lógica de transformación
- Pandas + openpyxl — procesamiento de datos
- psycopg2 — conexión al Data Warehouse
- Kaggle API — descarga automática de datasets

---

## Estructura del repositorio

```
test_data_engineer/
├── dags/
│   └── etl_ecommerce.py        # DAG principal — 8 tareas
├── scripts/
│   ├── explore_data.py          # Exploración de datasets
│   └── ddl_datamart.sql         # DDL tablas + índices
├── docs/
│   ├── consultas_negocio.sql    # 7 consultas de negocio
│   ├── decisiones_tecnicas.md   # Decisiones y justificaciones
│   └── diagrama_modelo.png      # ERD del modelo de datos
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md
```

---

## Levantar el entorno en 10 minutos

### Requisitos previos

- Docker y Docker Compose instalados
- Cuenta de Kaggle con API key ([obtenerla aquí](https://www.kaggle.com/settings/account))
- Acceso al servidor del Data Warehouse remoto

### Paso 1 — Clonar el repositorio

```bash
git clone <url-del-repositorio>
cd test_data_engineer
```

### Paso 2 — Configurar variables de entorno

```bash
cp .env.example .env
nano .env   # Completa los valores reales
```

Valores obligatorios en `.env`:

- `KAGGLE_USERNAME` y `KAGGLE_KEY` — de tu cuenta Kaggle
- `DW_PASSWORD` — contraseña del Data Warehouse remoto
- `AIRFLOW_ADMIN_PASSWORD` — la que quieras para la UI

### Paso 3 — Levantar los servicios

```bash
sudo docker compose up -d
```

Espera ~60 segundos y verifica que todos los contenedores están corriendo:

```bash
sudo docker compose ps
```

### Paso 4 — Acceder a la UI de Airflow

Abre [http://localhost:8080](http://localhost:8080) en tu navegador.

- Usuario: `admin`
- Contraseña: la que pusiste en `AIRFLOW_ADMIN_PASSWORD`

### Paso 5 — Configurar la Connection del DW

```bash
sudo docker compose exec airflow-scheduler airflow connections add 'dw_postgres' \
    --conn-type 'postgres' \
    --conn-host '15.204.173.204' \
    --conn-port '6432' \
    --conn-schema 'datamart_andres_dw' \
    --conn-login 'coder-ra-c6' \
    --conn-password 'TU_PASSWORD'
```

Verificar que quedó bien:

```bash
sudo docker compose exec airflow-scheduler airflow connections get dw_postgres
```

### Paso 6 — Configurar las Variables

```bash
sudo docker compose exec airflow-scheduler airflow variables set dw_password TU_PASSWORD
sudo docker compose exec airflow-scheduler airflow variables set dw_fx_rate_usd 1.20
sudo docker compose exec airflow-scheduler airflow variables set dw_default_country "United Kingdom"
```

Verificar:

```bash
sudo docker compose exec airflow-scheduler airflow variables get dw_fx_rate_usd
```

### Paso 7 — Ejecutar el pipeline

En la UI de Airflow:

1. Busca el DAG `etl_ecommerce`
2. Actívalo con el toggle izquierdo
3. Haz clic en **▶ Trigger DAG**

El pipeline tarda aproximadamente 30-40 minutos en procesar ~1.000.000 de filas de dos fuentes.

### Paso 8 — Verificar que los datos llegaron

```bash
sudo docker compose exec airflow-scheduler python -c "
import psycopg2
conn = psycopg2.connect(host='15.204.173.204', port=6432,
    dbname='datamart_andres_dw', user='coder-ra-c6', password='TU_PASSWORD')
cur = conn.cursor()
for tabla in ['dim_producto', 'fact_ventas', 'fact_devoluciones', 'log_rechazos']:
    cur.execute('SELECT COUNT(*) FROM ' + tabla)
    print(tabla + ': ' + str(cur.fetchone()[0]) + ' filas')
conn.close()
"
```

Resultado esperado:

```
dim_producto: ~4.600 filas
fact_ventas: ~765.000 filas
fact_devoluciones: ~17.000 filas
log_rechazos: ~575.000 filas
```

---

## Arquitectura del pipeline

El DAG `etl_ecommerce` tiene 8 tareas con las siguientes dependencias:

```
extract_ds1 ─┐
             ├──► transform ──► load_dim_producto ──► load_fact_ventas ──► load_fact_devoluciones ──► load_log_rechazos ──► notify
extract_ds2 ─┘
```

Las dos extracciones corren en **paralelo**, luego el transform unifica ambas fuentes y carga las 4 tablas del DW en secuencia.

| Tarea | Descripción | Duración aprox. |
|-------|-------------|-----------------|
| `extract_ds1` | Descarga carrie1/ecommerce-data desde Kaggle | ~5 seg |
| `extract_ds2` | Descarga lakshmi/online-retail-dataset desde Kaggle | ~5 seg |
| `transform` | Unifica, normaliza, aplica reglas de calidad, separa ventas/devoluciones | ~30 seg |
| `load_dim_producto` | Carga catálogo de productos con descripción canónica | ~5 seg |
| `load_fact_ventas` | Carga ~765.000 filas en lotes de 5.000 | ~12 min |
| `load_fact_devoluciones` | Carga ~17.000 filas de devoluciones | ~1 min |
| `load_log_rechazos` | Carga ~575.000 registros rechazados con motivo | ~3 min |
| `notify` | Imprime resumen final en el log | ~1 seg |

---

## Fuentes de datos

### DS1 — carrie1/ecommerce-data

- 541.909 filas brutas
- Formato: CSV, encoding latin1
- Columnas: `InvoiceNo`, `StockCode`, `Description`, `Quantity`, `InvoiceDate`, `UnitPrice`, `CustomerID`, `Country`
- Periodo: 2010-2011
- CustomerID nulo: 24.93%

### DS2 — lakshmi25npathi/online-retail-dataset

- 525.461 filas brutas
- Formato: Excel (.xlsx)
- Columnas equivalentes con nombres distintos (`Invoice`, `Price`, `Customer ID`)
- Periodo: 2009-2010 (se solapa con DS1 en diciembre 2010)
- CustomerID nulo: 20.54%

---

## Modelo de datos

### `dim_producto`

Catálogo normalizado de productos. Clave primaria: `codigo_producto`. La descripción canónica se elige por moda estadística (descripción más frecuente por código de producto en el dataset de ventas limpio).

### `fact_ventas`

Transacciones válidas (`quantity > 0`, `price > 0`, `customer_id` presente). Incluye `revenue_bruto` precalculado. Idempotencia garantizada por `clave_dedup = invoice_no + stock_code`.

### `fact_devoluciones`

Transacciones con `quantity < 0`, separadas de ventas para calcular revenue neto con un JOIN simple sin filtros condicionales.

### `log_rechazos`

Trazabilidad completa de cada registro descartado. Motivos posibles: `stockcode_interno`, `precio_invalido`, `sin_customer_id`. Incluye JSON original para auditoría y reprocesamiento futuro.

---

## Reglas de calidad aplicadas

| Regla | Criterio | Destino |
|-------|----------|---------|
| StockCode interno | No sigue patrón `^\d+[A-Z]?$` | `log_rechazos` |
| Precio inválido | `unit_price <= 0` | `log_rechazos` |
| Sin cliente | `customer_id` es nulo | `log_rechazos` |
| Devolución | `quantity < 0` | `fact_devoluciones` |
| Venta válida | Pasa todas las reglas anteriores | `fact_ventas` |

---

## Idempotencia

El DAG es idempotente por tres mecanismos:

1. `ON CONFLICT (clave_dedup) DO NOTHING` en `fact_ventas` y `fact_devoluciones`
2. `ON CONFLICT (codigo_producto) DO UPDATE` en `dim_producto`
3. Lotes de 5.000 filas con conexión independiente por lote — evita timeouts en cargas grandes

Ejecutar el DAG dos veces el mismo día con los mismos datos produce exactamente el mismo estado en el repositorio analítico.

---

## Evidencia de carga final

Pipeline ejecutado el 2026-06-24. Resultado verificado contra el DW remoto:

| Tabla | Filas cargadas |
|-------|---------------|
| `dim_producto` | 4.616 |
| `fact_ventas` | 765.056 |
| `fact_devoluciones` | 17.417 |
| `log_rechazos` | 575.316 |

Total procesado: **1.067.370 filas brutas** → 782.473 cargadas → 575.316 rechazadas con trazabilidad completa.

---

## Comandos útiles

### Ver estado de las tareas del último run

```bash
sudo docker compose exec airflow-scheduler airflow tasks states-for-dag-run \
    etl_ecommerce <run_id>
```

### Leer log de una tarea específica

```bash
sudo docker compose exec airflow-scheduler find /opt/airflow/logs \
    -name "*.log" | grep <nombre_tarea> | tail -1 | \
    xargs sudo docker compose exec airflow-scheduler tail -50
```

### Reiniciar los contenedores

```bash
sudo docker compose down && sudo docker compose up -d
```

### Verificar variables de Airflow

```bash
sudo docker compose exec airflow-scheduler airflow variables list
```

---

*Andrés Guerra · Ingeniería de Datos · RIWI · Cohorte 6 · 2026*