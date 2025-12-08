# 1. Imports
import duckdb
import pandas as pd
from pathlib import Path

# 2. Rutas
BASE_DIR = Path(__file__).parent.parent
OUTPUT_DIR = BASE_DIR / "data" / "outputs"

# 3. Función principal
def ejecutar_transformaciones():
    con = duckdb.connect()
    con.execute(f"CREATE TABLE customers AS SELECT * FROM '{OUTPUT_DIR / 'clientes_limpios.csv'}'")
    con.execute(f"CREATE TABLE items AS SELECT * FROM '{OUTPUT_DIR / 'items_limpios.csv'}'")
    con.execute(f"CREATE TABLE orders AS SELECT * FROM '{OUTPUT_DIR / 'ordenes_limpios.csv'}'")

    print("✅ Tablas cargadas en DuckDB")

    # Query principal con CTE
    query = """
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
    ORDER BY customer_id, mes
    """
    
    # Ejecutar y guardar resultado
    resultado = con.execute(query).fetchdf() #este permite pasar de duckdb a pandas para luego pandas a csv
    resultado.to_csv(OUTPUT_DIR / "ventas_mensuales.csv", index=False)
    
    print(f"✅ Generado: ventas_mensuales.csv con {len(resultado)} registros")
    con.close()
    print("✅ Transformaciones completadas.")




# 4. Llamar a la función
ejecutar_transformaciones()