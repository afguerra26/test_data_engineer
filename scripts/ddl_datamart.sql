-- ══════════════════════════════════════════════════════════════
-- DDL — Repositorio Analítico DataMart S.A.S.
-- Base de datos: datamart_andres_dw
-- ══════════════════════════════════════════════════════════════

-- Dimensión de productos
-- Contiene el catálogo normalizado con descripción canónica
-- (la más frecuente por código de producto en las transacciones)
CREATE TABLE IF NOT EXISTS dim_producto (
    codigo_producto     VARCHAR(20)     PRIMARY KEY,
    nombre_canonico     VARCHAR(255)    NOT NULL,
    categoria           VARCHAR(100)    NOT NULL DEFAULT 'sin_categoria',
    pais_origen         VARCHAR(100),
    activo              BOOLEAN         NOT NULL DEFAULT TRUE,
    fuente_categoria    VARCHAR(100),
    fecha_actualizacion TIMESTAMP       DEFAULT NOW()
);

-- Tabla de hechos — ventas válidas
-- Solo transacciones con quantity > 0, price > 0 y customer_id presente
-- Idempotencia garantizada por clave_dedup (invoice_no + stock_code)
CREATE TABLE IF NOT EXISTS fact_ventas (
    id              BIGSERIAL       PRIMARY KEY,
    invoice_no      VARCHAR(20)     NOT NULL,
    codigo_producto VARCHAR(20)     NOT NULL REFERENCES dim_producto(codigo_producto),
    customer_id     VARCHAR(20),
    pais            VARCHAR(100),
    fecha           DATE            NOT NULL,
    cantidad        INTEGER         NOT NULL,
    precio_unitario NUMERIC(12,4)   NOT NULL,
    revenue_bruto   NUMERIC(14,4)   NOT NULL,
    fuente_dataset  VARCHAR(100),
    clave_dedup     VARCHAR(60)     NOT NULL UNIQUE,
    fecha_carga     TIMESTAMP       DEFAULT NOW()
);

-- Tabla de hechos — devoluciones
-- Transacciones con quantity < 0, separadas para calcular neto
-- monto almacenado como positivo para facilitar cálculos
CREATE TABLE IF NOT EXISTS fact_devoluciones (
    id              BIGSERIAL       PRIMARY KEY,
    invoice_no      VARCHAR(20)     NOT NULL,
    codigo_producto VARCHAR(20)     NOT NULL REFERENCES dim_producto(codigo_producto),
    customer_id     VARCHAR(20),
    pais            VARCHAR(100),
    fecha           DATE            NOT NULL,
    cantidad        INTEGER         NOT NULL,
    precio_unitario NUMERIC(12,4)   NOT NULL,
    monto           NUMERIC(14,4)   NOT NULL,
    fuente_dataset  VARCHAR(100),
    clave_dedup     VARCHAR(60)     NOT NULL UNIQUE,
    fecha_carga     TIMESTAMP       DEFAULT NOW()
);

-- Log de registros rechazados
-- Trazabilidad completa de cada registro descartado con motivo
CREATE TABLE IF NOT EXISTS log_rechazos (
    id              BIGSERIAL       PRIMARY KEY,
    fuente_dataset  VARCHAR(100),
    motivo          VARCHAR(100)    NOT NULL,
    invoice_no      VARCHAR(20),
    codigo_producto VARCHAR(20),
    datos_originales JSONB,
    fecha_proceso   TIMESTAMP       DEFAULT NOW()
);

-- ── Índices para consultas analíticas ──────────────────────────

-- fact_ventas: filtros frecuentes por fecha, país y producto
CREATE INDEX IF NOT EXISTS idx_ventas_fecha
    ON fact_ventas(fecha);
CREATE INDEX IF NOT EXISTS idx_ventas_producto
    ON fact_ventas(codigo_producto);
CREATE INDEX IF NOT EXISTS idx_ventas_pais
    ON fact_ventas(pais);
CREATE INDEX IF NOT EXISTS idx_ventas_customer
    ON fact_ventas(customer_id);

-- fact_devoluciones: mismos filtros
CREATE INDEX IF NOT EXISTS idx_devol_fecha
    ON fact_devoluciones(fecha);
CREATE INDEX IF NOT EXISTS idx_devol_producto
    ON fact_devoluciones(codigo_producto);

-- log_rechazos: filtrar por motivo y fuente
CREATE INDEX IF NOT EXISTS idx_rechazos_motivo
    ON log_rechazos(motivo);
CREATE INDEX IF NOT EXISTS idx_rechazos_fuente
    ON log_rechazos(fuente_dataset);