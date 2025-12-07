# Semana 1 - Diseño de Pipeline de Datos

**Proyecto:** Pipeline de E-Commerce con Olist Dataset
**Alumno**: Esteban López
**Fecha**: Septiembre - Diciembre 2025

## Objetivo del proyecto

El objetivo de este proyecto es dar un paso inicial al mundo de ingenería de datos, los pipelines, ETLs, python, SQL y demás con un dataset de práctica, con el fin de interiorizar los fundamentos y el pensamiento crítico como Data Engineer. 

## Dataset Utilizado

**Fuente:** Brazilian E-Commerce Public Dataset by Olist (Kaggle)

**Descripción:** Dataset público con transacciones reales de e-commerce brasileño entre 2016-2018.

**Archivos principales:**

| Archivo | Registros | Descripción |
|---------|-----------|-------------|
| `olist_customers_dataset.csv` | 99,441 | Información demográfica de clientes: customer_id (único por orden), customer_unique_id (cliente real), código postal, ciudad y estado |
| `olist_orders_dataset.csv` | 99,441 | Ciclo de vida de órdenes: order_id, customer_id, estado de la orden (delivered, shipped, canceled), timestamps de compra, aprobación y entrega |
| `olist_order_items_dataset.csv` | 112,650 | Detalle de productos por orden: order_id, product_id, seller_id, precio unitario, costo de envío (freight_value) |

**Relación entre tablas:**
Las tablas se conectan con el ID del cliente, el ID de orden y el ID del producto

## Arquitectura del Pipeline
El pipeline consta de 6 etapas principales que transforman los datos crudos en información lista para análisis:

### Diagrama de Flujo
```
CSV raw → ADQUISICIÓN → INGESTA → LIMPIEZA → TRANSFORMACIÓN → VALIDACIÓN → EXPORTACIÓN
```

### Descripción de Etapas

**1. ADQUISICIÓN**
- **Descripción:** Se descargan los archivos manualmente y se colocan en la carpeta data/raw
- **Herramienta:** Kaggle

**2. INGESTA**
- **Descripción:** Con pandas.read_csv() se leen los archivos
- **Herramienta:** pandas
- **Input:** CSV files
- **Output:** DataFrames en memoria

**3. LIMPIEZA DE DATOS**
- **Descripción:** Se manejan nulos, tipos de datos, duplicados innecesarios
- **Herramienta:** pandas

**4. TRANSFORMACIÓN**
- **Descripción:** Se usa DuckDB para leer archivos de SQL sin un motor de base de datos como tal y realizar JOINS
- **Herramienta:** DuckDB + SQL
- **Output:** Tablas procesadas

**5. VALIDACIÓN**
- **Descripción:** Que las tablas tengan sentido para BI
- **Herramienta:** pandas + Python
- **Ejemplo:** Verificar rangos de precios, coherencia temporal

**6. EXPORTACIÓN**
- **Descripción:** Guardar resultados finales
- **Herramienta:** pandas.to_csv() / to_parquet()
- **Output:** Archivos procesados en `data/outputs/`

## Herramientas y Justificación

| Etapa | Herramienta | Justificación |
|-------|-------------|---------------|
| Adquisición | Kaggle | Fuente confiable de datasets públicos |
| Ingesta | `pandas.read_csv()` | Lectura eficiente de CSV con parsing automático |
| Limpieza | `pandas` | Funciones especializadas: `.drop_duplicates()`, `.astype()`, etc |
| Transformación | **DuckDB + SQL** | JOINs 50-100x más rápidos que pandas, sintaxis SQL estándar |
| Validación | `pandas + Python` | Lógica personalizada para reglas de negocio complejas |
| Exportación | `.to_csv()` / `.to_parquet()` | Formatos estándar para consumo en herramientas de BI |

---

## Conclusión

Este diseño permite procesar 100k+ transacciones de forma eficiente, escalable y mantenible. La estructura modular facilita agregar nuevas validaciones o fuentes de datos en el futuro.

**Repositorio:** [Agregar link de GitHub cuando esté disponible]