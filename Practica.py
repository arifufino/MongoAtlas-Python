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

# ===================== CONSOLIDACIÓN BASE =====================
doc_base = pd.concat([doc1, doc2, doc3, doc4], ignore_index=True)

# ===================== GENERAR PRODUCTOS 500 - 599 =====================
np.random.seed(42)

ids = range(500, 600)

df_500_599 = pd.DataFrame({
    "id": ids,
    "nombre": [f"Producto_{i}" for i in ids],
    "precio_compra": np.round(np.random.uniform(1.5, 10.0, 100), 2),
    "categoria": np.random.choice(doc_base["categoria"].dropna().unique(), 100),
    "stock": np.random.randint(1, 200, 100),
    "proveedor": np.random.choice(doc_base["proveedor"].dropna().unique(), 100)
})

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

# ===================== CONSOLIDACIÓN FINAL =====================
doc = pd.concat([doc1, doc2, doc3, doc4, df_500_599], ignore_index=True)

print("\nPrimeros registros del DataFrame consolidado:")
print(doc.head(10))

print("\nInformación del DataFrame:")
print(doc.info())

# ===================== LIMPIEZA DE DATOS =====================
doc["id"] = pd.to_numeric(doc["id"], errors="coerce")
doc["precio_compra"] = pd.to_numeric(doc["precio_compra"], errors="coerce")
doc["stock"] = pd.to_numeric(doc["stock"], errors="coerce")
doc["precio_venta_publico"] = pd.to_numeric(doc["precio_venta_publico"], errors="coerce")

print("\nValores nulos por columna:")
print(doc.isnull().sum())

# ===================== IMPUTACIÓN =====================
mediana_precio = doc["precio_compra"].median()
doc["precio_compra"].fillna(mediana_precio, inplace=True)

margen = 0.095
doc["precio_venta_publico"].fillna(doc["precio_compra"] * (1 + margen), inplace=True)

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
uri = "mongodb+srv://USUARIO:CONTRASEÑA@clusterproductos.o1jmgy5.mongodb.net/?retryWrites=true&w=majority"
client = MongoClient(uri)

db = client["inventario"]
coleccion = db["productos"]

print("\nCOMPROBACIÓN DE CONEXIÓN A MONGODB")
print("Documentos actuales en la colección:", coleccion.count_documents({}))

# ===================== EVITAR DUPLICADOS =====================
coleccion.delete_many({"_id": {"$gte": 500, "$lte": 599}})

# ===================== INSERCIÓN EN MONGODB =====================
docs_mongo = df_500_599.copy()
docs_mongo["_id"] = docs_mongo["id"]
docs_mongo.drop(columns=["id"], inplace=True)

coleccion.insert_many(docs_mongo.to_dict("records"))

print("\nProductos 500–599 insertados correctamente en MongoDB")

print(
    "Verificación MongoDB (500–599):",
    coleccion.count_documents({"_id": {"$gte": 500, "$lte": 599}})
)

# ===================== VISUALIZACIÓN (DATAFRAME CONSOLIDADO) =====================

# Histograma precios de compra
plt.figure(figsize=(8,6))
sns.histplot(doc["precio_compra"], bins=20, kde=True)
plt.title("Histograma de precios de compra (DataFrame consolidado)")
plt.xlabel("Precio de compra")
plt.ylabel("Frecuencia")
plt.show()

# Dispersión compra vs venta
plt.figure(figsize=(8,6))
sns.scatterplot(x="precio_compra", y="precio_venta_publico", data=doc)
plt.title("Precio de compra vs Precio de venta (DataFrame consolidado)")
plt.xlabel("Precio de compra")
plt.ylabel("Precio de venta público")
plt.show()

# Stock total por categoría
plt.figure(figsize=(10,6))
stock_categoria = doc.groupby("categoria")["stock"].sum().reset_index()
sns.barplot(x="categoria", y="stock", data=stock_categoria)
plt.xticks(rotation=45)
plt.title("Stock total por categoría (DataFrame consolidado)")
plt.xlabel("Categoría")
plt.ylabel("Stock total")
plt.show()

# Boxplot margen porcentual por categoría
plt.figure(figsize=(10,6))
sns.boxplot(x="categoria", y="margen_porcentual", data=doc)
plt.xticks(rotation=45)
plt.title("Distribución del margen porcentual por categoría (DataFrame consolidado)")
plt.xlabel("Categoría")
plt.ylabel("Margen porcentual")
plt.show()
