import duckdb
from pathlib import Path

#Rutas
BASE_DIR = Path(__file__).parent.parent
OUTPUT_DIR = BASE_DIR / "data" / "outputs"

def ejecutar_modelo():
    print("Iniciando modelo estrella...")

    con = duckdb.connect()

    print("Creando dim_clientes...")
    con.execute(f"""
        CREATE TABLE dim_clientes AS
        SELECT DISTINCT 
        customer_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state
        FROM '{OUTPUT_DIR / 'clientes_limpios.csv'}'
    """)

    print("Creando dim_productos...")
    con.execute(f"""
        CREATE TABLE dim_productos AS
        SELECT DISTINCT 
            product_id
        FROM '{OUTPUT_DIR / 'items_limpios.csv'}'
    """)

    print("Creando fact_ventas...")
    con.execute(f"""
        CREATE TABLE fact_ventas AS
        SELECT
            o.order_id,
            o.customer_id,
            i.product_id,
            o.order_purchase_timestamp AS fecha_compra,
            i.price,
            i.freight_value,
            (i.price + i.freight_value) AS ingreso_total
        FROM '{OUTPUT_DIR / 'ordenes_limpios.csv'}' o
        JOIN '{OUTPUT_DIR / 'items_limpios.csv'}' i
            ON o.order_id = i.order_id
        JOIN dim_clientes c
            ON o.customer_id = c.customer_id
    """)

    print("\nValidando integridad referencial...")
    
    clientes_sin = con.execute("""
        SELECT COUNT(*) 
        FROM fact_ventas
        WHERE customer_id NOT IN (SELECT customer_id FROM dim_clientes) 
    """).fetchone()[0] #Trae la fila 0 y como es una tupla, la saca de ahí y nos da el valor 
    
    productos_sin = con.execute("""
        SELECT COUNT(*) 
        FROM fact_ventas
        WHERE product_id NOT IN (SELECT product_id FROM dim_productos)
    """).fetchone()[0]
    
    if clientes_sin == 0:
        print("✅ Todos los clientes en fact_ventas existen en dim_clientes")
    else:
        print(f"⚠️ Hay {clientes_sin} clientes huérfanos")
    
    if productos_sin == 0:
        print("✅ Todos los productos en fact_ventas existen en dim_productos")
    else:
        print(f"⚠️ Hay {productos_sin} productos huérfanos")

    print("\nExportando tablas...")
    
    dim_clientes = con.execute("SELECT * FROM dim_clientes").fetchdf()
    dim_clientes.to_csv(OUTPUT_DIR / "dim_clientes.csv", index=False)
    print(f"✅ dim_clientes.csv con {len(dim_clientes)} registros")
    
    dim_productos = con.execute("SELECT * FROM dim_productos").fetchdf()
    dim_productos.to_csv(OUTPUT_DIR / "dim_productos.csv", index=False)
    print(f"✅ dim_productos.csv con {len(dim_productos)} registros")
    
    fact_ventas = con.execute("SELECT * FROM fact_ventas").fetchdf()
    fact_ventas.to_csv(OUTPUT_DIR / "fact_ventas.csv", index=False)
    print(f"✅ fact_ventas.csv con {len(fact_ventas)} registros")
    
    con.close()
    print("\n✅ Modelo estrella completado.")

ejecutar_modelo()