# ===================== LIBRERÍAS =====================
import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
import seaborn as sns
from pymongo import MongoClient
import numpy as np

# ===================== CARGA DE DOCUMENTOS =====================

# Documento JSON
doc1 = pd.read_json("productos.json")

# Documento TXT
doc2 = pd.read_csv("productos.txt")

# Documento XML
doc3 = pd.read_xml("productos.xml")

# Documento SQLite
conn = sqlite3.connect("productos.db")
doc4 = pd.read_sql_query("SELECT * FROM productos", conn)
conn.close()

# ===================== CONSOLIDACIÓN =====================
doc = pd.concat([doc1, doc2, doc3, doc4], ignore_index=True)

print("Primeros registros:")
print(doc.head(10))

print("\nInformación del DataFrame:")
print(doc.info())

# ===================== LIMPIEZA DE DATOS =====================

# Conversión de columnas numéricas
doc["id"] = pd.to_numeric(doc["id"], errors="coerce")
doc["precio_compra"] = pd.to_numeric(doc["precio_compra"], errors="coerce")
doc["stock"] = pd.to_numeric(doc["stock"], errors="coerce")
doc["precio_venta_publico"] = pd.to_numeric(doc["precio_venta_publico"], errors="coerce")

print("\nValores nulos por columna:")
print(doc.isnull().sum())

# Porcentaje de datos sucios
total = len(doc)
porcentaje_nulos = (doc.isnull().sum() / total) * 100
print("\nPorcentaje de datos nulos:")
print(porcentaje_nulos.round(2), "%")

# ===================== IMPUTACIÓN =====================

# Mediana para precio_compra
mediana_precio = doc["precio_compra"].median()
doc["precio_compra"].fillna(mediana_precio, inplace=True)

# Precio de venta = precio_compra * margen
margen = 0.095
doc["precio_venta_publico"].fillna(doc["precio_compra"] * (1 + margen), inplace=True)

# Completar texto faltante
doc["categoria"].fillna("Sin categoría", inplace=True)
doc["proveedor"].fillna("Proveedor desconocido", inplace=True)
doc["nombre"].fillna("Producto sin nombre", inplace=True)
doc["stock"].fillna(0, inplace=True)

# ===================== ESTADÍSTICAS =====================
for col in ["precio_compra", "precio_venta_publico", "stock"]:
    print(f"\nEstadísticas de {col}:")
    print("Mínimo:", doc[col].min())
    print("Máximo:", doc[col].max())
    print("Media:", doc[col].mean())
    print("Mediana:", doc[col].median())
    print("Desviación estándar:", doc[col].std())

# ===================== CONEXIÓN A MONGODB ATLAS =====================
uri = "mongodb+srv://admin_productos:1234qwer@clusterproductos.o1jmgy5.mongodb.net/?retryWrites=true&w=majority"
client = MongoClient(uri)

db = client["inventario"]
coleccion = db["productos"]

print("\nCOMPROBACIÓN DE CONEXIÓN A MONGODB")
print("Documentos actuales en la colección:", coleccion.count_documents({}))

# ===================== GENERAR PRODUCTOS 500 - 599 =====================
np.random.seed(42)

ids = range(500, 600)

df_500_599 = pd.DataFrame({
    "id": ids,
    "nombre": [f"Producto_{i}" for i in ids],
    "precio_compra": np.round(np.random.uniform(1.5, 10.0, 100), 2),
    "categoria": np.random.choice(doc["categoria"].unique(), 100),
    "stock": np.random.randint(1, 200, 100),
    "proveedor": np.random.choice(doc["proveedor"].unique(), 100)
})

# Precio venta y márgenes
margen = 0.12
df_500_599["precio_venta_publico"] = np.round(
    df_500_599["precio_compra"] * (1 + margen), 2
)

df_500_599["margen_absoluto"] = (
    df_500_599["precio_venta_publico"] - df_500_599["precio_compra"]
)

df_500_599["margen_porcentual"] = (
    df_500_599["margen_absoluto"] / df_500_599["precio_compra"]
)

print("\nProductos 500–599 generados:")
print(df_500_599.head())

# ===================== INSERCIÓN EN MONGODB =====================

# MongoDB usa _id
docs_mongo = df_500_599.copy()
docs_mongo["_id"] = docs_mongo["id"]
docs_mongo.drop(columns=["id"], inplace=True)

coleccion.insert_many(docs_mongo.to_dict("records"))

print("\nProductos 500–599 insertados correctamente en MongoDB")

# ===================== VERIFICACIÓN FINAL =====================
print(
    "Productos 500–599 en MongoDB:",
    coleccion.count_documents({"_id": {"$gte": 500, "$lte": 599}})
)
