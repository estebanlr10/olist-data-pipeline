import logging
from datetime import datetime
from pathlib import Path

# Importar las funciones de tus módulos
from src.ingesta import ejecutar_pipeline as ejecutar_ingesta
from src.transformaciones import ejecutar_transformaciones
from src.modelo import ejecutar_modelo
from src.validaciones import validar_calidad_datos

# Configurar logging
BASE_DIR = Path(__file__).parent
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    filename=LOG_DIR / "pipeline.log",
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def ejecutar_pipeline_completo():
    """
    Orquesta la ejecución completa del pipeline de datos.
    """
    logging.info("="*50)
    logging.info("INICIANDO PIPELINE COMPLETO")
    logging.info("="*50)
    
    try:
        # Etapa 1: Ingesta y Limpieza
        logging.info("Etapa 1: Ingesta y Limpieza")
        ejecutar_ingesta()
        logging.info("✅ Ingesta completada")
        
        # Etapa 2: Transformaciones SQL
        logging.info("Etapa 2: Transformaciones SQL")
        ejecutar_transformaciones()
        logging.info("✅ Transformaciones completadas")
        
        # Etapa 3: Modelo Estrella
        logging.info("Etapa 3: Modelo Estrella")
        ejecutar_modelo()
        logging.info("✅ Modelo completado")

        # Etapa 4: Validaciones de Calidad
        logging.info("Etapa 4: Validaciones de Calidad")
        validar_calidad_datos()
        logging.info("✅ Validaciones completadas")
        
        logging.info("="*50)
        logging.info("✅ PIPELINE COMPLETADO EXITOSAMENTE")
        logging.info("="*50)
        
        print("\n✅ Pipeline ejecutado correctamente. Revisa logs/pipeline.log para detalles.\n")
        
    except Exception as e:
        logging.error(f"❌ ERROR EN EL PIPELINE: {str(e)}")
        print(f"\n❌ Error: {str(e)}")
        print("Revisa logs/pipeline.log para más detalles.\n")
        raise

if __name__ == "__main__":
    ejecutar_pipeline_completo()