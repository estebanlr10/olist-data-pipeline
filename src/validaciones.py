import pandas as pd
import logging
from pathlib import Path
from datetime import datetime

# Rutas del proyecto
BASE_DIR = Path(__file__).parent.parent
OUTPUT_DIR = BASE_DIR / "data" / "outputs"
LOG_DIR = BASE_DIR / "logs"


def validar_calidad_datos():
    """
    Valida la calidad de los datos procesados.
    Si encuentra errores críticos, detiene el pipeline.
    """
    logging.info("="*50)
    logging.info("INICIANDO VALIDACIONES DE CALIDAD")
    logging.info("="*50)
    
    errores = 0
    
    # Leer datos finales
    try:
        fact_ventas = pd.read_csv(OUTPUT_DIR / "fact_ventas.csv")
        dim_clientes = pd.read_csv(OUTPUT_DIR / "dim_clientes.csv")
        logging.info("✅ Archivos cargados correctamente")
    except Exception as e:
        logging.error(f"❌ Error al cargar archivos: {e}")
        raise Exception("No se pudieron cargar los archivos para validar")
    
    # 1. Validar valores nulos en columnas críticas
    logging.info("Validacion 1: Nulos en columnas criticas")
    if fact_ventas[["order_id", "customer_id", "product_id"]].isnull().any().any():
        logging.warning("⚠️ Hay valores nulos en columnas criticas de fact_ventas")
        errores += 1
    else:
        logging.info("✅ No hay nulos en columnas criticas")
    
    # 2. Validar fechas futuras
    logging.info("Validacion 2: Fechas futuras")
    fact_ventas["fecha_compra"] = pd.to_datetime(fact_ventas["fecha_compra"])
    if (fact_ventas["fecha_compra"] > datetime.now()).any():
        logging.warning("⚠️ Hay fechas de compra en el futuro")
        errores += 1
    else:
        logging.info("✅ No hay fechas futuras")
    
    # 3. Validar precios negativos
    logging.info("Validacion 3: Precios negativos")
    if (fact_ventas["price"] < 0).any():
        logging.warning("⚠️ Hay precios negativos")
        errores += 1
    else:
        logging.info("✅ No hay precios negativos")
    
    # 4. Validar rangos de precios (0 a 10,000 reales)
    logging.info("Validacion 4: Rangos de precios")
    if not fact_ventas["price"].between(0, 10000).all():
        logging.warning("⚠️ Hay precios fuera de rango (0-10,000)")
        errores += 1
    else:
        logging.info("✅ Todos los precios estan en rango valido")
    
    # 5. Validar cantidad de registros mínima
    logging.info("Validacion 5: Cantidad minima de registros")
    if len(fact_ventas) < 1000:
        logging.warning(f"⚠️ Muy pocos registros: {len(fact_ventas)}")
        errores += 1
    else:
        logging.info(f"✅ Cantidad de registros OK: {len(fact_ventas)}")
    
    # Resultado final
    logging.info("="*50)
    if errores == 0:
        logging.info("✅ TODAS LAS VALIDACIONES PASARON")
        logging.info("="*50)
        print("\n✅ Validaciones de calidad completadas exitosamente.\n")
    else:
        logging.error(f"❌ VALIDACIONES FALLIDAS. Errores encontrados: {errores}")
        logging.error("="*50)
        raise Exception(f"Validaciones de calidad fallaron. Se encontraron {errores} errores.")

if __name__ == "__main__":
    validar_calidad_datos()