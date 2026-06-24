-- ══════════════════════════════════════════════════════════════
-- CONSULTAS DE NEGOCIO — DataMart S.A.S.
-- Repositorio analítico: datamart_andres_dw
-- ══════════════════════════════════════════════════════════════

-- 1. Evolución mensual de ventas netas (ventas - devoluciones)
-- ─────────────────────────────────────────────────────────────
SELECT
    DATE_TRUNC('month', v.fecha::date) AS mes,
    SUM(v.revenue_bruto)               AS revenue_ventas,
    COALESCE(SUM(d.monto), 0)          AS revenue_devoluciones,
    SUM(v.revenue_bruto) - COALESCE(SUM(d.monto), 0) AS revenue_neto
FROM fact_ventas v
LEFT JOIN fact_devoluciones d
    ON  DATE_TRUNC('month', v.fecha::date) = DATE_TRUNC('month', d.fecha::date)
    AND v.codigo_producto = d.codigo_producto
GROUP BY 1
ORDER BY 1;

-- 2. Revenue bruto por categoría y proporción de devoluciones
-- ─────────────────────────────────────────────────────────────
SELECT
    p.categoria,
    SUM(v.revenue_bruto)                                    AS revenue_bruto,
    COALESCE(SUM(ABS(d.monto)), 0)                          AS total_devoluciones,
    ROUND(
        COALESCE(SUM(ABS(d.monto)), 0) /
        NULLIF(SUM(v.revenue_bruto), 0) * 100, 2
    )                                                       AS pct_devolucion
FROM fact_ventas v
JOIN dim_producto p ON v.codigo_producto = p.codigo_producto
LEFT JOIN fact_devoluciones d ON v.codigo_producto = d.codigo_producto
GROUP BY p.categoria
ORDER BY revenue_bruto DESC;

-- 3a. Top 10 productos por revenue neto
-- ─────────────────────────────────────────────────────────────
SELECT
    v.codigo_producto,
    p.nombre_canonico,
    SUM(v.revenue_bruto)                        AS revenue_bruto,
    COALESCE(SUM(ABS(d.monto)), 0)              AS devoluciones,
    SUM(v.revenue_bruto) - COALESCE(SUM(ABS(d.monto)), 0) AS revenue_neto
FROM fact_ventas v
JOIN dim_producto p ON v.codigo_producto = p.codigo_producto
LEFT JOIN fact_devoluciones d ON v.codigo_producto = d.codigo_producto
GROUP BY v.codigo_producto, p.nombre_canonico
ORDER BY revenue_neto DESC
LIMIT 10;

-- 3b. Top 10 productos con mayor tasa de devolución
-- ─────────────────────────────────────────────────────────────
SELECT
    v.codigo_producto,
    p.nombre_canonico,
    SUM(v.cantidad)                             AS unidades_vendidas,
    COALESCE(SUM(ABS(d.cantidad)), 0)           AS unidades_devueltas,
    ROUND(
        COALESCE(SUM(ABS(d.cantidad)), 0)::numeric /
        NULLIF(SUM(v.cantidad), 0) * 100, 2
    )                                           AS tasa_devolucion_pct
FROM fact_ventas v
JOIN dim_producto p ON v.codigo_producto = p.codigo_producto
LEFT JOIN fact_devoluciones d ON v.codigo_producto = d.codigo_producto
GROUP BY v.codigo_producto, p.nombre_canonico
HAVING SUM(v.cantidad) > 100
ORDER BY tasa_devolucion_pct DESC
LIMIT 10;

-- 4. Países con más transacciones y ticket promedio
-- ─────────────────────────────────────────────────────────────
SELECT
    pais,
    COUNT(DISTINCT invoice_no)              AS total_facturas,
    SUM(cantidad)                           AS total_unidades,
    SUM(revenue_bruto)                      AS revenue_total,
    ROUND(SUM(revenue_bruto) /
          NULLIF(COUNT(DISTINCT invoice_no), 0), 2) AS ticket_promedio
FROM fact_ventas
GROUP BY pais
ORDER BY revenue_total DESC
LIMIT 15;

-- 5. Comportamiento clientes identificados vs sin customer ID
-- (aplica porque excluimos sin customer_id — se documenta el impacto)
-- ─────────────────────────────────────────────────────────────
SELECT
    CASE
        WHEN customer_id IS NULL OR customer_id = 'nan' THEN 'sin_identificar'
        ELSE 'identificado'
    END                                     AS tipo_cliente,
    COUNT(DISTINCT invoice_no)              AS total_facturas,
    COUNT(*)                                AS total_lineas,
    SUM(revenue_bruto)                      AS revenue_total,
    ROUND(AVG(revenue_bruto), 2)            AS ticket_linea_promedio
FROM fact_ventas
GROUP BY 1;

-- 6. Productos sin descripción consistente y total códigos únicos
-- ─────────────────────────────────────────────────────────────
SELECT
    v.codigo_producto,
    COUNT(DISTINCT v.invoice_no)            AS total_transacciones,
    p.nombre_canonico                       AS descripcion_canonica
FROM fact_ventas v
LEFT JOIN dim_producto p ON v.codigo_producto = p.codigo_producto
WHERE p.nombre_canonico IS NULL
   OR p.nombre_canonico = 'NAN'
   OR LENGTH(p.nombre_canonico) < 3
GROUP BY v.codigo_producto, p.nombre_canonico
ORDER BY total_transacciones DESC
LIMIT 20;

-- Total códigos únicos de producto
SELECT COUNT(DISTINCT codigo_producto) AS total_codigos_unicos
FROM dim_producto;

-- 7. Recomendación concreta al equipo de producto
-- Top productos con alto revenue pero alta tasa de devolución
-- (oportunidad de mejora de calidad de producto)
-- ─────────────────────────────────────────────────────────────
SELECT
    v.codigo_producto,
    p.nombre_canonico,
    SUM(v.revenue_bruto)                                        AS revenue_bruto,
    COALESCE(SUM(ABS(d.monto)), 0)                              AS monto_devuelto,
    ROUND(COALESCE(SUM(ABS(d.monto)), 0) /
          NULLIF(SUM(v.revenue_bruto), 0) * 100, 2)             AS pct_devolucion,
    SUM(v.revenue_bruto) - COALESCE(SUM(ABS(d.monto)), 0)      AS revenue_neto
FROM fact_ventas v
JOIN dim_producto p ON v.codigo_producto = p.codigo_producto
LEFT JOIN fact_devoluciones d ON v.codigo_producto = d.codigo_producto
GROUP BY v.codigo_producto, p.nombre_canonico
HAVING SUM(v.revenue_bruto) > 1000
   AND ROUND(COALESCE(SUM(ABS(d.monto)), 0) /
             NULLIF(SUM(v.revenue_bruto), 0) * 100, 2) > 20
ORDER BY monto_devuelto DESC
LIMIT 10;