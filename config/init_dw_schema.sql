-- ════════════════════════════════════════════════════════
-- Esquema del repositorio analítico — DataMart S.A.S.
-- Se ejecuta automáticamente al crear el contenedor postgres-dw
-- (Postgres corre todo *.sql en /docker-entrypoint-initdb.d/ una sola vez,
--  cuando el volumen de datos está vacío).
-- ════════════════════════════════════════════════════════

-- ── Dimensión: producto ──
-- Catálogo normalizado. Se puebla desde las transacciones (no hay API en el
-- alcance obligatorio) y, si se implementa el plus, se enriquece desde la API.
CREATE TABLE IF NOT EXISTS dim_producto (
    codigo_producto     VARCHAR(50) PRIMARY KEY,
    nombre_canonico     VARCHAR(255),
    categoria           VARCHAR(100),
    pais_origen         VARCHAR(100),
    activo              BOOLEAN DEFAULT TRUE,
    fuente_categoria    VARCHAR(50),  -- 'api' | 'estatico' | 'inferido'
    fecha_actualizacion TIMESTAMP DEFAULT now()
);

-- ── Hechos: ventas válidas (cantidad > 0, precio > 0) ──
CREATE TABLE IF NOT EXISTS fact_ventas (
    id                  BIGSERIAL PRIMARY KEY,
    invoice_no          VARCHAR(50) NOT NULL,
    codigo_producto     VARCHAR(50) NOT NULL,
    customer_id         VARCHAR(50),          -- NULL permitido a propósito (ver decisiones técnicas)
    pais                VARCHAR(100),
    fecha               DATE NOT NULL,         -- estandarizada a UTC
    cantidad            INTEGER NOT NULL,
    precio_unitario     NUMERIC(12,2) NOT NULL,
    revenue_bruto        NUMERIC(14,2) NOT NULL,
    fuente_dataset      VARCHAR(50) NOT NULL,  -- 'kaggle_ventas_actuales' | 'kaggle_historico'
    clave_dedup         VARCHAR(150) NOT NULL, -- invoice_no + codigo_producto + fecha, evita duplicados entre fuentes
    fecha_carga         TIMESTAMP DEFAULT now(),
    UNIQUE (clave_dedup)
);

-- ── Hechos: devoluciones / ajustes (cantidad <= 0) ──
CREATE TABLE IF NOT EXISTS fact_devoluciones (
    id                  BIGSERIAL PRIMARY KEY,
    invoice_no          VARCHAR(50) NOT NULL,
    codigo_producto     VARCHAR(50) NOT NULL,
    customer_id         VARCHAR(50),
    pais                VARCHAR(100),
    fecha               DATE NOT NULL,
    cantidad            INTEGER NOT NULL,      -- se guarda negativa, tal cual viene
    precio_unitario     NUMERIC(12,2) NOT NULL,
    monto               NUMERIC(14,2) NOT NULL, -- cantidad * precio_unitario (negativo)
    fuente_dataset      VARCHAR(50) NOT NULL,
    clave_dedup         VARCHAR(150) NOT NULL,
    fecha_carga         TIMESTAMP DEFAULT now(),
    UNIQUE (clave_dedup)
);

-- ── Log de rechazos: todo registro que no cumple las reglas de negocio ──
CREATE TABLE IF NOT EXISTS log_rechazos (
    id                  BIGSERIAL PRIMARY KEY,
    fuente_dataset      VARCHAR(50) NOT NULL,
    motivo              VARCHAR(255) NOT NULL,
    invoice_no          VARCHAR(50),
    codigo_producto     VARCHAR(50),
    datos_originales    JSONB,
    fecha_proceso       TIMESTAMP DEFAULT now()
);

-- Índices para las consultas de negocio (sección 7)
CREATE INDEX IF NOT EXISTS idx_ventas_fecha       ON fact_ventas (fecha);
CREATE INDEX IF NOT EXISTS idx_ventas_producto     ON fact_ventas (codigo_producto);
CREATE INDEX IF NOT EXISTS idx_ventas_pais         ON fact_ventas (pais);
CREATE INDEX IF NOT EXISTS idx_devoluciones_fecha   ON fact_devoluciones (fecha);
CREATE INDEX IF NOT EXISTS idx_devoluciones_producto ON fact_devoluciones (codigo_producto);

-- ── Vista: revenue neto por producto y por día ──
-- Resuelve directamente la regla de negocio "revenue neto = bruto - devoluciones
-- del mismo código de producto en el mismo periodo diario".
CREATE OR REPLACE VIEW vw_revenue_diario_producto AS
SELECT
    COALESCE(v.codigo_producto, d.codigo_producto) AS codigo_producto,
    COALESCE(v.fecha, d.fecha)                     AS fecha,
    COALESCE(v.revenue_bruto_dia, 0)                AS revenue_bruto,
    COALESCE(d.monto_devoluciones_dia, 0)           AS monto_devoluciones,
    COALESCE(v.revenue_bruto_dia, 0) + COALESCE(d.monto_devoluciones_dia, 0) AS revenue_neto
FROM (
    SELECT codigo_producto, fecha, SUM(revenue_bruto) AS revenue_bruto_dia
    FROM fact_ventas
    GROUP BY codigo_producto, fecha
) v
FULL OUTER JOIN (
    SELECT codigo_producto, fecha, SUM(monto) AS monto_devoluciones_dia
    FROM fact_devoluciones
    GROUP BY codigo_producto, fecha
) d
ON v.codigo_producto = d.codigo_producto AND v.fecha = d.fecha;
