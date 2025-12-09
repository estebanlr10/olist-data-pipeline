# 🚀 Olist E-Commerce Data Pipeline

Pipeline de procesamiento de datos end-to-end para análisis de ventas de e-commerce brasileño.

## 📊 Descripción

Este proyecto implementa un pipeline completo de Data Engineering que procesa datos transaccionales de Olist (Kaggle), aplicando limpieza, transformaciones SQL, modelado dimensional y validaciones de calidad.

## 🏗️ Arquitectura del Pipeline
```
RAW DATA → INGESTA → LIMPIEZA → TRANSFORMACIONES → MODELO ESTRELLA → VALIDACIONES → OUTPUTS
```

**Etapas:**
1. **Ingesta y Limpieza** (pandas): Eliminación de nulos, duplicados, normalización
2. **Transformaciones SQL** (DuckDB): Agregaciones y métricas de negocio
3. **Modelo Estrella**: Tablas dimensionales y de hechos
4. **Validaciones**: Integridad referencial y calidad de datos

## 📁 Estructura del Proyecto
```
ProyectoFinal/
├── data/
│   ├── raw/              # Datos originales (CSV)
│   └── outputs/          # Datos procesados
├── src/
│   ├── ingesta.py        # Limpieza de datos
│   ├── transformaciones.py  # SQL queries
│   ├── modelo.py         # Modelo dimensional
│   └── validaciones.py   # Validaciones de calidad
├── logs/                 # Logs de ejecución
├── docs/                 # Documentación
├── run_pipeline.py       # Orquestador principal
└── requirements.txt      # Dependencias
```

## 🚀 Cómo Ejecutar

### 1. Clonar el repositorio
```bash
git clone https://github.com/estebanlr10/olist-data-pipeline.git
cd olist-data-pipeline
```

### 2. Instalar dependencias
```bash
pip install -r requirements.txt
```

### 3. Colocar datos raw
Descargar el [Brazilian E-Commerce Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) y colocar los CSVs en `data/raw/`

### 4. Ejecutar pipeline completo
```bash
python run_pipeline.py
```

## 📈 Outputs Generados

El pipeline genera 7 archivos CSV en `data/outputs/`:

**Datos Limpios:**
- `clientes_limpios.csv`
- `items_limpios.csv`
- `ordenes_limpias.csv`

**Modelo Estrella:**
- `dim_clientes.csv`
- `dim_productos.csv`
- `fact_ventas.csv`

**Reportes:**
- `ventas_mensuales.csv`

## 🛠️ Tecnologías

- **Python 3.x**
- **pandas**: Manipulación de datos
- **DuckDB**: Motor SQL analítico
- **pathlib**: Gestión de rutas
- **logging**: Trazabilidad

## 📝 Logs

Todos los eventos del pipeline se registran en `logs/pipeline.log` con timestamps.

## 👤 Autor

**Esteban López**  
Bootcamp de Data Engineering - Ian Saura  

