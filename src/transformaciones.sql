CREATE OR REPLACE TABLE customers AS
SELECT * FROM '../Outputs/clientes_limpios.csv';

CREATE OR REPLACE TABLE items AS
SELECT * FROM '../Outputs/items_limpios.csv';

CREATE OR REPLACE TABLE orders AS
SELECT * FROM '../Outputs/ordenes_limpias.csv';

COPY (
    WITH fact_items AS (
        SELECT
            o.order_id,
            o.customer_id,
            c.customer_city,
            c.customer_state,
            o.order_purchase_timestamp,
            i.order_item_id,
            i.product_id,
            i.price,
            i.freight_value,
            (i.price + i.freight_value) AS ingreso_item
        FROM orders o
        JOIN items i ON o.order_id = i.order_id
        JOIN customers c ON o.customer_id = c.customer_id
    )
    SELECT 
        customer_id,
        STRFTIME('%Y-%m', order_purchase_timestamp) AS mes,
        SUM(ingreso_item) AS ingresos_totales,
        COUNT(*) AS Cantidad_Ventas
    FROM fact_items
    GROUP BY customer_id, mes


) TO '../Outputs/ventas_mensuales.csv' 
WITH (HEADER, DELIMITER ',');