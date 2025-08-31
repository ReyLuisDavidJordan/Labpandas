# lab_tabular/src/lab_tabular.py

import os
import pandas as pd
import duckdb

# ---------------------------
# Rutas base
# ---------------------------

BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # Carpeta del script
DATA_DIR = os.path.join(BASE_DIR, "../data")           # Carpeta de datos
RESULTS_DIR = os.path.join(BASE_DIR, "../results")     # Carpeta de resultados
os.makedirs(RESULTS_DIR, exist_ok=True)               # Crear results si no existe

CSV_PATH = os.path.join(DATA_DIR, "datos.csv")

# ---------------------------
# 1) Lectura y exploración con pandas
# ---------------------------

df = pd.read_csv(CSV_PATH)

print("Primeras filas:")
print(df.head())

print("\nInformación general:")
print(df.info())

print("\nNulos por columna:")
print(df.isna().sum())

# ---------------------------
# 2) Columnas derivadas y limpieza
# ---------------------------

df['total'] = df['precio'] * df['cantidad']
df['categoria'] = df['categoria'].str.lower()
df['cantidad'] = df['cantidad'].fillna(0)

# ---------------------------
# 3) Agrupaciones y filtros
# ---------------------------

df_filtrado = df[df['categoria'].isin(['electronica', 'ropa'])]

resumen_pandas = (
    df_filtrado.groupby('categoria')
    .agg(
        total_ventas=('total', 'sum'),
        promedio_cantidad=('cantidad', 'mean')
    )
    .reset_index()
)

print("\nResumen pandas:")
print(resumen_pandas)

resumen_pandas.to_csv(os.path.join(RESULTS_DIR, "resumen_pandas.csv"), index=False)

# ---------------------------
# 4) DuckDB: análisis SQL
# ---------------------------

con = duckdb.connect()

query1 = f"""
SELECT categoria, COUNT(*) AS num_filas
FROM '{CSV_PATH}'
GROUP BY categoria
"""
df_duck_count = con.execute(query1).df()
print("\nConteo por categoría (DuckDB):")
print(df_duck_count)

query2 = f"""
SELECT 
    LOWER(categoria) AS categoria,
    SUM(precio * cantidad) AS total_ventas,
    AVG(cantidad) AS promedio_cantidad
FROM '{CSV_PATH}'
WHERE LOWER(categoria) IN ('electronica','ropa')
GROUP BY LOWER(categoria)
"""
resumen_duck = con.execute(query2).df()
print("\nResumen DuckDB:")
print(resumen_duck)

resumen_duck.to_csv(os.path.join(RESULTS_DIR, "resumen_duckdb.csv"), index=False)
