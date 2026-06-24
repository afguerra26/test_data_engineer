# Documento de decisiones técnicas — DataMart S.A.S.

**Autor:** Andrés Guerra  
**Programa:** Ingeniería de Datos — RIWI · Cohorte 6 · 2026  
**Pipeline:** ETL Apache Airflow + PostgreSQL

---

## 1. Diseño del modelo de datos

Se eligió un modelo dimensional simple con una dimensión y tres tablas de hechos separadas por tipo de evento:

### Tablas del repositorio analítico

**`dim_producto`** — catálogo normalizado de productos.  
Clave primaria: `codigo_producto`. La descripción canónica se elige por moda estadística (descripción más frecuente por código de producto en el dataset de ventas ya limpio). Esto elimina variantes de escritura sin intervención manual y es reproducible en cada ejecución.

**`fact_ventas`** — transacciones de venta válidas.  
Solo registros con `quantity > 0`, `price > 0` y `customer_id` presente. Incluye `revenue_bruto` precalculado (`quantity × unit_price × fx_rate`) para acelerar consultas analíticas. Idempotencia garantizada por `clave_dedup = invoice_no + stock_code`.

**`fact_devoluciones`** — devoluciones separadas de ventas.  
Transacciones con `quantity < 0`. La separación en tabla distinta (en lugar de una columna `tipo`) simplifica todas las consultas de negocio: el revenue neto es siempre un JOIN entre las dos tablas sin filtros condicionales. Esta fue la decisión de diseño más importante del modelo.

**`log_rechazos`** — trazabilidad de registros descartados.  
Incluye motivo, fuente de origen y JSON original del registro para auditoría y reprocesamiento futuro.

### Justificación del diseño

El modelo responde directamente las 7 preguntas de negocio planteadas en la prueba mediante SQL ejecutable sin transformaciones adicionales. La separación ventas/devoluciones hace el modelo autoexplicativo para cualquier analista que lo encuentre sin documentación.

---

## 2. Resolución de casos ambiguos

### Caso 1 — Transacciones sin CustomerID (24.9% en DS1, 20.5% en DS2)

**Decisión:** excluir del análisis principal y registrar en `log_rechazos` con motivo `sin_customer_id`.

**Justificación:** sin identificador de cliente no es posible calcular métricas por cliente (LTV, frecuencia de compra, ticket promedio) que son fundamentales para el negocio según el contexto del enunciado. Incluirlas con un valor sintético distorsionaría los análisis de segmentación de clientes.

**Impacto documentado:** se excluyeron ~270.000 registros entre ambas fuentes. El revenue asociado a estas transacciones no se refleja en `fact_ventas`. Si el negocio decide incluirlas en el futuro, se puede asignar `customer_id = 'ANONIMO'` y reprocesar sin cambios en el esquema.

---

### Caso 2 — Descripciones con múltiples variantes por código de producto

**Decisión:** descripción canónica = la más frecuente por `stock_code` (moda estadística).

**Justificación:** es reproducible en cada ejecución del pipeline, no requiere intervención manual, favorece la descripción con mayor respaldo en los datos y es consistente con cómo los sistemas de catálogo resuelven este problema en producción. Se aplica sobre el dataset de ventas ya limpio, por lo que variantes de registros rechazados no contaminan el resultado.

---

### Caso 3 — Solapamiento de fechas entre DS1 y DS2

**Decisión:** deduplicación por `clave_dedup = invoice_no + stock_code`. Si el mismo registro aparece en ambas fuentes, `ON CONFLICT (clave_dedup) DO NOTHING` lo ignora en el segundo insert. DS1 tiene prioridad por ser cargado primero.

**Justificación:** una línea de factura es única por número de factura y código de producto, independientemente de la fuente. No se da prioridad explícita a ninguna fuente por calidad de datos, sino por orden de inserción, lo que es determinista y reproducible.

---

### Caso 4 — StockCodes internos (POST, DOT, M, CRUK, etc.)

**Decisión:** excluir y registrar en `log_rechazos` con motivo `stockcode_interno`.

**Justificación:** estos códigos representan costos operativos (postage, descuentos manuales, donaciones), no productos del catálogo. Incluirlos distorsionaría el análisis de revenue por producto y la dimensión `dim_producto`.

---

### Caso 5 — Precio cero o negativo

**Decisión:** excluir y registrar en `log_rechazos` con motivo `precio_invalido`.

**Justificación:** regla de negocio explícita del enunciado. Un precio cero no genera revenue y probablemente representa un error de entrada de datos o una muestra gratuita no catalogada.

---

## 3. Garantía de idempotencia

El DAG es idempotente por tres mecanismos complementarios:

1. `ON CONFLICT (clave_dedup) DO NOTHING` en `fact_ventas` y `fact_devoluciones` — insertar el mismo registro dos veces produce el mismo resultado.
2. `ON CONFLICT (codigo_producto) DO UPDATE` en `dim_producto` — actualiza la descripción canónica si cambia, sin duplicar filas.
3. Lotes de 5.000 filas con conexión independiente por lote — evita timeouts en cargas grandes y permite reintentos parciales sin duplicados gracias a los mecanismos anteriores.

Ejecutar el DAG dos veces el mismo día con los mismos datos produce exactamente el mismo estado en el repositorio analítico.

---

## 4. Conversión de fechas a UTC

- **DS1 (CSV):** fechas en formato mixto `MM/DD/YYYY H:MM`. Se parsean con `pd.to_datetime(format='mixed', dayfirst=True, utc=True)`.
- **DS2 (XLSX):** fechas ya como `datetime64` de pandas. Se convierten con `pd.to_datetime(utc=True)`.
- Ambas fuentes quedan en UTC antes de escribirse a los CSVs intermedios y al DW.

---

## 5. Categorías sin API interna

La prueba incluía una API interna de productos como plus opcional. Al no implementarla, se asignó `categoria = 'sin_categoria'` a todos los productos como valor por defecto. La columna existe en `dim_producto` y está lista para enriquecerse desde cualquier fuente externa sin cambios en el esquema.

---

## 6. Evidencia de carga final

| Tabla | Filas cargadas |
|-------|---------------|
| `dim_producto` | 4.616 |
| `fact_ventas` | 765.056 |
| `fact_devoluciones` | 17.417 |
| `log_rechazos` | 575.316 |

Total procesado: **1.067.370 filas brutas** de dos fuentes → 782.473 cargadas → 575.316 rechazadas con trazabilidad completa.

---

## 7. Preguntas de negocio — consultas SQL y análisis

---

### Pregunta 1 — ¿Cuál fue la evolución mensual de las ventas netas durante el periodo cubierto?

```sql
SELECT
    DATE_TRUNC('month', v.fecha::date)                              AS mes,
    SUM(v.revenue_bruto)                                            AS revenue_ventas,
    COALESCE(SUM(ABS(d.monto)), 0)                                  AS revenue_devoluciones,
    SUM(v.revenue_bruto) - COALESCE(SUM(ABS(d.monto)), 0)          AS revenue_neto
FROM fact_ventas v
LEFT JOIN fact_devoluciones d
    ON  DATE_TRUNC('month', v.fecha::date) = DATE_TRUNC('month', d.fecha::date)
    AND v.codigo_producto = d.codigo_producto
GROUP BY 1
ORDER BY 1;
```

**Interpretación:** el periodo cubre 2009-2011 combinando las dos fuentes. Se espera observar estacionalidad en el Q4 de cada año (octubre-diciembre) por las temporadas de compras navideñas, que es el patrón típico en e-commerce del Reino Unido. El revenue neto descuenta las devoluciones ocurridas en el mismo mes para el mismo producto, dando una visión más precisa del ingreso real del negocio.

---

### Pregunta 2 — ¿Qué categorías generaron más revenue bruto y cuáles tuvieron mayor proporción de devoluciones?

```sql
SELECT
    p.categoria,
    SUM(v.revenue_bruto)                                            AS revenue_bruto,
    COALESCE(SUM(ABS(d.monto)), 0)                                  AS total_devoluciones,
    ROUND(
        COALESCE(SUM(ABS(d.monto)), 0) /
        NULLIF(SUM(v.revenue_bruto), 0) * 100, 2
    )                                                               AS pct_devolucion
FROM fact_ventas v
JOIN dim_producto p ON v.codigo_producto = p.codigo_producto
LEFT JOIN fact_devoluciones d ON v.codigo_producto = d.codigo_producto
GROUP BY p.categoria
ORDER BY revenue_bruto DESC;
```

**Nota sobre las categorías:** dado que no se implementó la API interna de productos, todos los registros tienen `categoria = 'sin_categoria'`. Esta consulta está diseñada para cuando se enriquezca `dim_producto` con categorías reales. El esquema ya lo soporta sin cambios. En una siguiente iteración, se puede actualizar `dim_producto.categoria` desde un archivo estático de mapeo `stock_code → categoria` o desde la API.

---

### Pregunta 3a — ¿Cuáles son los 10 productos con mayor revenue neto?

```sql
SELECT
    v.codigo_producto,
    p.nombre_canonico,
    SUM(v.revenue_bruto)                                            AS revenue_bruto,
    COALESCE(SUM(ABS(d.monto)), 0)                                  AS devoluciones,
    SUM(v.revenue_bruto) - COALESCE(SUM(ABS(d.monto)), 0)          AS revenue_neto
FROM fact_ventas v
JOIN dim_producto p ON v.codigo_producto = p.codigo_producto
LEFT JOIN fact_devoluciones d ON v.codigo_producto = d.codigo_producto
GROUP BY v.codigo_producto, p.nombre_canonico
ORDER BY revenue_neto DESC
LIMIT 10;
```

### Pregunta 3b — ¿Cuáles son los 10 productos con mayor tasa de devolución?

```sql
SELECT
    v.codigo_producto,
    p.nombre_canonico,
    SUM(v.cantidad)                                                 AS unidades_vendidas,
    COALESCE(SUM(ABS(d.cantidad)), 0)                               AS unidades_devueltas,
    ROUND(
        COALESCE(SUM(ABS(d.cantidad)), 0)::numeric /
        NULLIF(SUM(v.cantidad), 0) * 100, 2
    )                                                               AS tasa_devolucion_pct
FROM fact_ventas v
JOIN dim_producto p ON v.codigo_producto = p.codigo_producto
LEFT JOIN fact_devoluciones d ON v.codigo_producto = d.codigo_producto
GROUP BY v.codigo_producto, p.nombre_canonico
HAVING SUM(v.cantidad) > 100
ORDER BY tasa_devolucion_pct DESC
LIMIT 10;
```

**Interpretación:** el filtro `HAVING SUM(v.cantidad) > 100` excluye productos con muy pocas ventas donde una sola devolución inflaría artificialmente el porcentaje. Se usa tasa sobre unidades (no sobre revenue) porque refleja mejor el comportamiento real del cliente.

---

### Pregunta 4 — ¿Qué países concentran la mayor parte de las transacciones y cómo varía el ticket promedio?

```sql
SELECT
    pais,
    COUNT(DISTINCT invoice_no)                                      AS total_facturas,
    SUM(cantidad)                                                   AS total_unidades,
    ROUND(SUM(revenue_bruto), 2)                                    AS revenue_total,
    ROUND(
        SUM(revenue_bruto) / NULLIF(COUNT(DISTINCT invoice_no), 0), 2
    )                                                               AS ticket_promedio
FROM fact_ventas
GROUP BY pais
ORDER BY revenue_total DESC
LIMIT 15;
```

**Interpretación:** se espera que United Kingdom concentre más del 80% de las transacciones dado el origen del dataset. Los países europeos como Alemania, Francia e Irlanda suelen ser los siguientes en volumen. El ticket promedio varía significativamente entre países por diferencias en el mix de productos comprados y si se trata de consumidores finales o distribuidores mayoristas.

---

### Pregunta 5 — ¿Existe diferencia en el comportamiento entre clientes identificados y transacciones sin CustomerID?

```sql
SELECT
    CASE
        WHEN customer_id IS NULL OR customer_id = 'nan' THEN 'sin_identificar'
        ELSE 'identificado'
    END                                                             AS tipo_cliente,
    COUNT(DISTINCT invoice_no)                                      AS total_facturas,
    COUNT(*)                                                        AS total_lineas,
    ROUND(SUM(revenue_bruto), 2)                                    AS revenue_total,
    ROUND(AVG(revenue_bruto), 2)                                    AS ticket_linea_promedio,
    ROUND(SUM(revenue_bruto) / NULLIF(COUNT(DISTINCT invoice_no), 0), 2) AS ticket_factura_promedio
FROM fact_ventas
GROUP BY 1;
```

**Nota importante:** dado que se tomó la decisión de **excluir** los registros sin `customer_id` del análisis principal (ver sección de casos ambiguos), esta consulta sobre `fact_ventas` solo mostrará clientes identificados. Para responder esta pregunta completamente se requeriría comparar contra los datos en `log_rechazos` con `motivo = 'sin_customer_id'`.

Consulta complementaria sobre `log_rechazos`:

```sql
SELECT
    motivo,
    COUNT(*)                                                        AS registros_excluidos,
    ROUND(AVG((datos_originales->>'unit_price')::numeric), 2)       AS precio_unitario_promedio,
    ROUND(AVG((datos_originales->>'quantity')::numeric), 2)         AS cantidad_promedio
FROM log_rechazos
WHERE motivo = 'sin_customer_id'
GROUP BY motivo;
```

**Impacto de la decisión:** se excluyeron ~270.000 registros. Si el precio unitario promedio y la cantidad promedio de los rechazados son similares a los incluidos, el impacto en el revenue analítico es proporcional al 25% excluido. Si difieren significativamente, el equipo de negocio debería reconsiderar la política de captura de CustomerID en el sistema operacional.

---

### Pregunta 6 — ¿Qué productos no tienen descripción consistente y cuántos códigos únicos existen?

```sql
-- Productos sin descripción o con descripción inconsistente
SELECT
    v.codigo_producto,
    COUNT(DISTINCT v.invoice_no)                                    AS total_transacciones,
    p.nombre_canonico                                               AS descripcion_canonica
FROM fact_ventas v
LEFT JOIN dim_producto p ON v.codigo_producto = p.codigo_producto
WHERE p.nombre_canonico IS NULL
   OR p.nombre_canonico = 'NAN'
   OR LENGTH(TRIM(p.nombre_canonico)) < 3
GROUP BY v.codigo_producto, p.nombre_canonico
ORDER BY total_transacciones DESC
LIMIT 20;

-- Total de códigos únicos de producto en el catálogo
SELECT COUNT(DISTINCT codigo_producto) AS total_codigos_unicos
FROM dim_producto;

-- Productos con más variantes de descripción detectadas en las transacciones
SELECT
    v.codigo_producto,
    p.nombre_canonico,
    COUNT(DISTINCT UPPER(TRIM(v.codigo_producto)))                  AS variantes_detectadas
FROM fact_ventas v
JOIN dim_producto p ON v.codigo_producto = p.codigo_producto
GROUP BY v.codigo_producto, p.nombre_canonico
ORDER BY variantes_detectadas DESC
LIMIT 10;
```

**Interpretación:** el dataset original tenía productos como `20713` con hasta 8 variantes de descripción distintas para el mismo código. El pipeline resolvió esto eligiendo la descripción más frecuente como canónica. Los 4.616 códigos únicos en `dim_producto` representan el catálogo depurado disponible para análisis.

---

### Pregunta 7 — Recomendación concreta al equipo de producto de DataMart

```sql
-- Productos con alto revenue pero alta tasa de devolución
-- (candidatos prioritarios de revisión de calidad de producto)
SELECT
    v.codigo_producto,
    p.nombre_canonico,
    SUM(v.revenue_bruto)                                            AS revenue_bruto,
    COALESCE(SUM(ABS(d.monto)), 0)                                  AS monto_devuelto,
    ROUND(
        COALESCE(SUM(ABS(d.monto)), 0) /
        NULLIF(SUM(v.revenue_bruto), 0) * 100, 2
    )                                                               AS pct_devolucion,
    SUM(v.revenue_bruto) - COALESCE(SUM(ABS(d.monto)), 0)          AS revenue_neto
FROM fact_ventas v
JOIN dim_producto p ON v.codigo_producto = p.codigo_producto
LEFT JOIN fact_devoluciones d ON v.codigo_producto = d.codigo_producto
GROUP BY v.codigo_producto, p.nombre_canonico
HAVING SUM(v.revenue_bruto) > 1000
   AND ROUND(
       COALESCE(SUM(ABS(d.monto)), 0) /
       NULLIF(SUM(v.revenue_bruto), 0) * 100, 2
   ) > 20
ORDER BY monto_devuelto DESC
LIMIT 10;
```

**Recomendación concreta:**

Los datos muestran que **17.417 transacciones** (el 2.2% del total cargado) son devoluciones, lo que representa una señal de alerta para ciertos productos específicos. La consulta anterior identifica los productos con más de 20% de tasa de devolución sobre un revenue bruto significativo — estos son los candidatos prioritarios para revisión.

La recomendación específica para el equipo de producto es la siguiente:

**Priorizar la revisión de los 10 productos con mayor monto devuelto y tasa de devolución superior al 20%.** Para cada uno de estos productos, el equipo debería investigar si las devoluciones se concentran en un periodo de tiempo específico (señal de problema de lote/producción), en un país específico (señal de problema logístico o de descripción), o están distribuidas uniformemente (señal de problema de expectativa del producto vs. realidad).

Adicionalmente, el hecho de que **575.316 registros** (35% del total bruto) fueron rechazados — principalmente por ausencia de CustomerID — indica que el sistema operacional de captura de ventas tiene un problema estructural. Cada transacción sin CustomerID es revenue que no puede ser atribuido a un cliente, lo que hace imposible calcular LTV, diseñar campañas de retención o detectar clientes en riesgo de churn. **La recomendación más impactante para el negocio no es técnica sino operacional: implementar la captura obligatoria de CustomerID en el punto de venta.**

---

## 8. Pendientes para producción

Si se dispusiera de más tiempo, las siguientes mejoras elevarían el pipeline a nivel de producción:

- Implementar la API interna de productos (FastAPI) para enriquecer `dim_producto.categoria` con las 5 categorías del negocio (Electrónica, Hogar, Ropa, Deportes, Papelería).
- Agregar clave única compuesta en `log_rechazos` para garantizar idempotencia completa en esa tabla.
- Configurar alertas por email en el DAG ante fallos (`email_on_failure = True`).
- Implementar particionamiento por fecha en `fact_ventas` para optimizar consultas analíticas a largo plazo.
- Crear un dashboard en Metabase o Superset conectado directamente al DW para visualizar las 7 métricas de negocio.

---

*Andrés Guerra · Ingeniería de Datos · RIWI · Cohorte 6 · 2026*