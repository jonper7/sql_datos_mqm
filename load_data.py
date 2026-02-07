import pandas as pd
import psycopg2
from psycopg2 import extras
import os
import numpy as np

# Configuración
DB_CONFIG = {
    'host': 'localhost',
    'port': '5432',
    'database': 'JPBase',
    'user': 'jonper',
    'password': 'jonper'
}
CARPETA = 'datos_mqm'
ARCHIVO = 'datos.xlsx'
SHEET_NAME = 'datos'
SCHEMA_NAME = 'M_FISICO'
TABLE_NAME = 'mqm_calidad_agua'

# Ruta relativa
ruta_archivo = os.path.join(CARPETA, ARCHIVO)

# Conectar a PostgreSQL
conn = psycopg2.connect(**DB_CONFIG)
cursor = conn.cursor()

# Leer Excel sin convertir NaN a string
df = pd.read_excel(ruta_archivo, sheet_name=SHEET_NAME, keep_default_na=True)
print(f"Cargadas {len(df)} filas del Excel")

# Limpiar datos: convertir NaN strings, espacios y valores nulos a None
def limpiar_celda(val):
    # Si es NaN de pandas, convertir a None
    if pd.isna(val):
        return None
    
    # Si es string
    if isinstance(val, str):
        val_limpio = val.strip()
        # Si es vacío o "NaN" (cualquier caso), convertir a None
        if val_limpio == '' or val_limpio.upper() == 'NAN':
            return None
        # Intentar convertir a número (reemplazando coma por punto)
        try:
            return float(val_limpio.replace(',', '.'))
        except:
            return val_limpio
    
    # Si es número, devolverlo como está
    return val

# Aplicar limpieza a todas las celdas
datos = [[limpiar_celda(val) for val in row] for row in df.values.tolist()]

# Crear query de inserción
columnas = ', '.join([f'"{col}"' for col in df.columns])
placeholders = ', '.join(['%s'] * len(df.columns))
insert_query = f'INSERT INTO "{SCHEMA_NAME}"."{TABLE_NAME}" ({columnas}) VALUES ({placeholders})'

# Insertar datos en lote
extras.execute_batch(cursor, insert_query, datos, page_size=1000)
conn.commit()

print(f"✓ {len(df)} filas insertadas en {SCHEMA_NAME}.{TABLE_NAME}")

# Cerrar conexión
cursor.close()
conn.close()