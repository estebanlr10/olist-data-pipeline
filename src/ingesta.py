import os
import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"
OUTPUT_DIR = BASE_DIR / "data" / "outputs"

os.system("cls")

#Solo para conocer el dataset
def lectura(dataset):
    df = pd.read_csv(dataset)
    pd.set_option('display.max_columns', None)
    print(f"Primeras 10 líneas:\n {df.head(10)}\n")
    print(f"Información clave del dataset:\n {df.info()}\n")
    
#Función orquestadora
def ejecutar_pipeline():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    clientes_csv(RAW_DIR / "olist_customers_dataset.csv")
    items_csv(RAW_DIR / "olist_order_items_dataset.csv")
    ordenes_csv(RAW_DIR / "olist_orders_dataset.csv")

#Limpieza clientes
def clientes_csv(dataset):

    df = pd.read_csv(dataset)

    #Cambio de tipo de dato a todas las columnas
    columnas = ['customer_id', 'customer_unique_id', 'customer_zip_code_prefix', 'customer_city','customer_state']
    df[columnas] = df[columnas].astype("string")
    print("✅ Tipos de datos cambiados exitosamente.")


    #Revisión y eliminación de IDs Únicos Duplicaos
    print(f"Cantidad de rows en dataset clientes: {len(df)}")
    if df['customer_unique_id'].duplicated().sum() == 0:
        print("✅ Columna customer_unique_id cuenta con datos ÚNICOS.")
    else: 
        print("❌ Columna customer_unique_id cuenta con IDs DUPLICADOS.")
        df = df.drop_duplicates(subset='customer_unique_id')
        print("✅ Por lo tanto los IDs duplicados han sido ELIMINADOS.")
        print(f"Cantidad de rows en dataset después de limpieza de duplicados: {len(df)}\n")

    #Revisión y eliminación de Nulos
    print(f"Cantidad de rows en data set: {len(df)}")   
    if df.isnull().sum().sum() == 0:
        print(f"✅ Dataset clientes NO cuenta con datos nulos.\n")
    else:
        print("❌ Dataset clientes tiene datos nulos.")
        df = df.dropna()
        print(f"Cantidad de rows en dataset DESPUÉS de limpieza de nulos: {len(df)}\n")

    #Evaluar si hay más de una longitud distinta en Zipcode (por ejemplo 4 digitos y 5 digitos)
    df['len_zip'] = df['customer_zip_code_prefix'].str.len()
    print(f"Cantidad de dígitos según los valores en la columna:\n{df['len_zip'].value_counts()}\n")
    if df['len_zip'].nunique() > 1:
        print("❌ Hay ZIP codes con longitudes DISTINTAS.")
        df['customer_zip_code_prefix'] = df['customer_zip_code_prefix'].str.zfill(5)
        df['len_zip'] = df['customer_zip_code_prefix'].str.len()
        print("✅ Todos los ZIP codes han sido TRANSFORMADOS a la misma longitud.\n")
    else:
        print("✅ Todos los ZIP codes tienen la misma longitud.\n")
    print(f"Cantidad de dígitos según los valores en la columna:\n{df['len_zip'].value_counts()}\n")

    # Eliminar columnas auxiliares antes de guardar
    df = df.drop(columns=['len_zip'])

    #Guardar cambios en un nuevo CSV y sin índice
    df.to_csv(OUTPUT_DIR / "clientes_limpios.csv", index=False)
    print("✅ Archivo: clientes_limpios.csv guardado en outputs.")
    print("=======================================================")

#Limpieza productos
def items_csv(dataset):

    df = pd.read_csv(dataset)

    #Cambio de tipo de dato a todas las columnas
    columnas_string = ['order_id','order_item_id','product_id','seller_id']
    df[columnas_string] = df[columnas_string].astype("string")
    df['shipping_limit_date'] = pd.to_datetime(df['shipping_limit_date'], errors='coerce')
    print("✅ Tipos de datos cambiados exitosamente.")

    
    #Verificación de nulos
    print(f"Cantidad de rows en dataset items: {len(df)}")
    if df.isnull().sum().sum() == 0:
        print("✅ El dataset items NO cuenta con valores nulos. \n")
    else:
        print(f"❌ El dataset items cuenta con {df.isnull().sum().sum()} valores nulos.")
        df = df.dropna()
        print("✅ Las rows valores nulos han sido ELIMINADOS.")
        print(f"Cantidad de rows después de limpieza {len(df)}\n")
    
    #Revision de valores negativos

    if (df['price'] < 0).any():
        print("❌ Hay items con precio MENOR a 0.")
        df = df[df["price"] > 0]
        print("✅ Items con precios negativos han sido ELIMINADOS.")
    else:
        print("✅ Items con precios POSITIVOS.")
    
    if (df['freight_value'] < 0).any():
        print("❌ Costos de transporte NEGATIVOS.")
        df = df[df['freight_value'] > 0]
        print("✅ Costos de transporte con precios negativos han sido ELIMINADOS.")
    else:
        print("✅ Costos de transporte con precios POSITIVOS.")


    #Guardar cambios en un nuevo CSV y sin índice
    df.to_csv(OUTPUT_DIR / "items_limpios.csv", index=False)
    print("✅ Archivo: items_limpios.csv guardado en outputs.")
    print("=======================================================")

#Limpieza ordenes
def ordenes_csv(dataset):

    df= pd.read_csv(dataset)

    #Cambio de tipo de datos 
    columnas_string = ['order_id', 'customer_id', 'order_status']
    df[columnas_string] = df[columnas_string].astype('string')
    df['order_purchase_timestamp'] = pd.to_datetime(df['order_purchase_timestamp'], errors='coerce')
    df['order_approved_at'] = pd.to_datetime(df['order_approved_at'], errors='coerce')
    df['order_delivered_carrier_date'] = pd.to_datetime(df['order_delivered_carrier_date'], errors='coerce')
    df['order_delivered_customer_date'] = pd.to_datetime(df['order_delivered_customer_date'], errors='coerce')
    df['order_estimated_delivery_date'] = pd.to_datetime(df['order_estimated_delivery_date'], errors='coerce')
    print("✅ Tipos de datos cambiados exitosamente")

    #Revisión de duplicados en order_id
    print("Cantidad de filas en dataset orders:", len(df))
    if df['order_id'].duplicated().sum() == 0:
        print("✅ Solo hay order_id ÚNICOS.\n")
    else:
        print("❌Hay order_id REPETIDOS.")
        df = df.drop_duplicates(subset='order_id')
        print("✅ Los order_id repetidos han sido ELIMINADOS.")

    # La aprobación no puede ocurrir antes de la compra
    invalid_approval = df[df['order_approved_at'] < df['order_purchase_timestamp']]
    if len(invalid_approval) > 0:
        print("❌ Hay órdenes que fueron aprobadas antes de ser compradas.\n")
    else:
        print("✅ No hay problemas en la aprobación de las compras.\n")

    #El carrier no puede recibir el pedido antes de la aprobación
    invalid_carrier = df[df['order_delivered_carrier_date'] < df['order_approved_at']]
    if len(invalid_carrier) > 0:
        print("❌ Hay órdenes entregadas al carrier antes de ser aprobadas.")
    else:
        print("✅ No hay problemas entre aprobación y envío al carrier.")

    #El cliente no puede recibir el pedido antes que el carrier
    invalid_customer_vs_carrier = df[df['order_delivered_customer_date'] < df['order_delivered_carrier_date']]

    if len(invalid_customer_vs_carrier) > 0:
        print("❌ Hay órdenes entregadas al cliente antes de que el carrier las reciba.")
    else:
        print("✅ No hay problemas entre entrega al carrier y al cliente.")
    
    #El cliente no puede recibir el pedido antes de comprarlo
    invalid_customer_vs_purchase = df[df['order_delivered_customer_date'] < df['order_purchase_timestamp']]

    if len(invalid_customer_vs_purchase) > 0:
        print("❌ Hay órdenes entregadas al cliente antes de la compra.")
    else:
        print("✅ No hay problemas entre compra y entrega al cliente.")

    #La fecha estimada no puede ser antes de la compra
    invalid_estimate_vs_purchase = df[df['order_estimated_delivery_date'] < df['order_purchase_timestamp']]

    if len(invalid_estimate_vs_purchase) > 0:
        print("❌ Hay fechas estimadas de entrega ANTERIORES a la compra.")
    else:
        print("✅ No hay problemas entre compra y fecha estimada de entrega.")
    
    df.to_csv(OUTPUT_DIR / "ordenes_limpios.csv", index=False)

    print("✅ Archivo: ordenes_limpias.csv guardado en outputs.")
    print("=======================================================")


#Llamado de función orquestadora
ejecutar_pipeline()