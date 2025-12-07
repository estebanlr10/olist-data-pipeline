--Clientes

CREATE OR REPLACE TABLE dim_clientes AS
SELECT DISTINCT 
    customer_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
FROM '../Outputs/clientes_limpios.csv';


--Productos

CREATE OR REPLACE TABLE dim_productos AS
SELECT DISTINCT 
    product_id
FROM '../Outputs/items_limpios.csv';


--Hechos

CREATE OR REPLACE TABLE fact_ventas AS
SELECT
    o.order_id,
    o.customer_id,
    i.product_id,
    o.order_purchase_timestamp AS fecha_compra,
    i.price,
    i.freight_value,
    (i.price + i.freight_value) AS ingreso_total
FROM '../Outputs/ordenes_limpias.csv' o
JOIN '../Outputs/items_limpios.csv' i
    ON o.order_id = i.order_id
JOIN dim_clientes c
    ON o.customer_id = c.customer_id;

SELECT * FROM dim_clientes LIMIT 10;
SELECT * FROM dim_productos LIMIT 10;
SELECT * FROM fact_ventas LIMIT 10;

SELECT COUNT(*) AS Clientes_SIN
FROM fact_ventas
WHERE customer_id NOT IN (SELECT customer_id FROM dim_clientes);

SELECT COUNT(*) AS Productos_SIN
FROM fact_ventas
WHERE product_id NOT IN (SELECT product_id FROM dim_productos);

