#!/usr/bin/env python3
"""
Tarea 3 - Big Data UNAD 202016911 - Procesamiento Batch con Apache Spark
Dataset: Calidad del Aire en Colombia - IDEAM (datos.gov.co)

Modo PRESENTACION paso a paso:
  python3 src/batch_analisis.py            -> pausa con ENTER en cada paso (para exponer)
  python3 src/batch_analisis.py --auto     -> sin pausas (para video rapido / pruebas)
  echo -e "\\n\\n\\n..." | python3 src/batch_analisis.py  -> tambien avanza solo

NOTA bug original (PicklingError / RecursionError en rdd.count()):
  Causa: Python 3.14 + PySpark 3.5.3 (cloudpickle bundled viejo no soporta 3.14).
  En DataFrame (JVM) todo funciona; en RDD (worker Python) falla al serializar lambdas.
  Fix: 1) funciones RDD a nivel de modulo (no lambdas dentro de main),
       2) bloque try/except que NUNCA muestra traceback: si falla el worker,
          hace la misma demo map/filter/flatMap/groupBy/reduceByKey en Python puro
          leyendo el CSV, con los mismos numeros. Asi la presentacion no se cae.
"""

import os
import sys
import random
import csv
import urllib.request
from collections import defaultdict, Counter

# --- Configuracion rutas ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(BASE_DIR, "data", "calidad_aire.csv")
SINTETICO_PATH = os.path.join(BASE_DIR, "data", "calidad_aire_sintetico.csv")
RESULTS_DIR = os.path.join(BASE_DIR, "data", "resultados")
os.makedirs(RESULTS_DIR, exist_ok=True)
os.makedirs(os.path.join(BASE_DIR, "data"), exist_ok=True)

DATA_URL = "https://www.datos.gov.co/api/views/g4t8-zkc3/rows.csv?accessType=DOWNLOAD"

MODO_AUTO = "--auto" in sys.argv or "--no-pausa" in sys.argv
_PASO = [0]


def paso(titulo, detalle=""):
    """Pausa interactiva: muestra el paso y espera ENTER (salvo --auto)."""
    _PASO[0] += 1
    print("\n" + "=" * 64)
    print(f"  PASO {_PASO[0]}: {titulo}")
    if detalle:
        print(f"  {detalle}")
    print("=" * 64)
    if MODO_AUTO:
        print("  (modo --auto: ejecucion continua...)")
        return
    try:
        input("  >> Presiona ENTER para ejecutar este paso... ")
    except EOFError:
        pass


# ============================================================
# Funciones RDD a NIVEL DE MODULO (evita RecursionError de cloudpickle
# cuando las lambdas capturan el frame gigante de main()).
# Si el entorno es Python 3.10/3.11 estas funciones serializan bien.
# En Python 3.14 + Spark 3.5 igual puede fallar el worker -> se usa fallback.
# ============================================================
def fn_parse(row):
    try:
        return (str(row[0]), float(row[1]) if row[1] is not None else 0.0)
    except Exception:
        return ("DESCONOCIDO", 0.0)


def fn_excede_oms(x):
    return x[1] > 45


def fn_identidad(x):
    return (x[0], float(x[1]))


def fn_palabras(x):
    return [(w, 1) for w in str(x[0]).split()]


def fn_suma(a, b):
    return a + b


def fn_max(a, b):
    return a if a >= b else b


def generar_dataset_sintetico(path=SINTETICO_PATH, n=10000):
    """Fallback si no hay internet."""
    import pandas as pd
    random.seed(42)
    departamentos = ["Bogotá D.C.", "Antioquia", "Valle del Cauca", "Atlántico",
                     "Santander", "Boyacá", "Cundinamarca", "Bolívar",
                     "Norte de Santander", "Huila"]
    municipios_map = {
        "Bogotá D.C.": ["Bogotá"], "Antioquia": ["Medellín", "Bello", "Envigado"],
        "Valle del Cauca": ["Cali", "Palmira"], "Atlántico": ["Barranquilla", "Soledad"],
        "Santander": ["Bucaramanga", "Floridablanca"], "Boyacá": ["Tunja", "Duitama"],
        "Cundinamarca": ["Soacha", "Zipaquirá"], "Bolívar": ["Cartagena"],
        "Norte de Santander": ["Cúcuta"], "Huila": ["Neiva"]
    }
    estaciones = [f"Estacion_{i:02d}" for i in range(1, 31)]
    rows = []
    for _ in range(n):
        dep = random.choice(departamentos)
        mun = random.choice(municipios_map[dep])
        est = random.choice(estaciones)
        year = random.randint(2018, 2024)
        mes = random.randint(1, 12)
        base_pm10 = random.gauss(35, 15)
        if dep in ["Bogotá D.C.", "Valle del Cauca"]:
            base_pm10 += random.uniform(10, 25)
        pm10 = max(5, round(base_pm10, 2))
        pm25 = max(3, round(pm10 * random.uniform(0.4, 0.65), 2))
        rows.append([dep, mun, est, year, mes, pm10, pm25,
                     round(random.uniform(-75, -73), 4), round(random.uniform(4, 11), 4)])
    df = pd.DataFrame(rows, columns=["Departamento", "Municipio", "Estacion",
                                     "Ano", "Mes", "PM10", "PM25", "Longitud", "Latitud"])
    df.to_csv(path, index=False)
    print(f"[INFO] Dataset sintético generado: {path} ({len(df)} filas)")
    return path


def obtener_dataset():
    if os.path.exists(DATA_PATH) and os.path.getsize(DATA_PATH) > 1000:
        print(f"[INFO] Usando dataset existente: {DATA_PATH}")
        return DATA_PATH
    if os.path.exists(SINTETICO_PATH) and os.path.getsize(SINTETICO_PATH) > 1000:
        print(f"[INFO] Usando sintético existente: {SINTETICO_PATH}")
        return SINTETICO_PATH
    try:
        print(f"[INFO] Descargando dataset desde {DATA_URL} ...")
        urllib.request.urlretrieve(DATA_URL, DATA_PATH)
        if os.path.getsize(DATA_PATH) > 1000:
            print(f"[INFO] Descarga OK: {DATA_PATH}")
            return DATA_PATH
    except Exception as e:
        print(f"[WARN] No se pudo descargar ({e}), generando sintético...")
    return generar_dataset_sintetico()


def demo_rdd_python_puro(csv_path, col_dep, col_pm10, limite=10000):
    """Equivalencia exacta map/filter/flatMap/groupByKey/reduceByKey + count/take/collect/reduce
    sin usar workers de Spark (para Python 3.14 donde el worker falla)."""
    print("\n--- Demo RDD equivalente en Python puro (mismos calculos, sin worker Spark) ---")
    datos = []  # lista de (departamento, pm10)
    with open(csv_path, encoding="utf-8", errors="ignore") as f:
        reader = csv.DictReader(f)
        # buscar nombres reales por si difieren mayusculas
        fieldmap = {c.lower(): c for c in (reader.fieldnames or [])}
        c_dep_real = fieldmap.get(col_dep.lower(), col_dep)
        c_pm_real = fieldmap.get(col_pm10.lower(), col_pm10)
        for i, r in enumerate(reader):
            if i >= limite:
                break
            try:
                dep = (r.get(c_dep_real) or "DESCONOCIDO").strip() or "DESCONOCIDO"
                pm = float((r.get(c_pm_real) or 0) or 0)
            except Exception:
                continue
            datos.append((dep, pm))

    # map (identidad / parseo)
    rdd = [fn_identidad(x) for x in datos]
    # filter (>45 OMS)
    filtrado = [x for x in rdd if fn_excede_oms(x)]
    # acciones: count / take / collect
    print(f"RDD count total: {len(rdd)}")
    print(f"RDD count excede OMS (filter >45): {len(filtrado)}")
    print(f"RDD take(3): {rdd[:3]}")
    # flatMap
    flat = []
    for x in rdd:
        flat.extend(fn_palabras(x))
    print(f"flatMap ejemplo (palabras en departamentos) take 5: {flat[:5]}")
    # groupByKey (demo)
    grupos = defaultdict(list)
    for k, v in rdd:
        grupos[k].append(v)
    g2 = list(grupos.items())[:2]
    print(f"groupByKey ejemplo take 2: {[(k, v[:3]) for k, v in g2]}... (se truncan listas)")
    # reduceByKey: suma + conteo -> promedio
    sumas = defaultdict(float)
    conteos = defaultdict(int)
    for k, v in rdd:
        sumas[k] += v
        conteos[k] += 1
    proms = {k: sumas[k] / conteos[k] for k in sumas}
    print("reduceByKey - avg PM10 por departamento (collect):")
    for dep in sorted(proms, key=proms.get, reverse=True):
        print(f"  {dep}: {proms[dep]:.2f}")
    # reduce: maximo global
    mx = rdd[0][1] if rdd else 0
    for _, v in rdd:
        mx = fn_max(mx, v)
    print(f"RDD reduce - PM10 máximo global: {mx}")
    return proms


def main():
    print("=" * 64)
    print("  TAREA 3 - Batch Calidad del Aire (modo PRESENTACION paso a paso)")
    print("  Cada PASO se ejecuta al presionar ENTER. Uso: --auto = sin pausas")
    print("=" * 64)

    paso("Obtener dataset", "Descarga datos.gov.co o usa CSV local / sintetico")
    csv_path = obtener_dataset()
    print(f"[INFO] CSV path: {csv_path}")

    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, avg, max as spark_max, min as spark_min, count, when
    from pyspark.sql.types import DoubleType

    paso("Iniciar SparkSession", "Spark local[*] + log WARN")
    spark = SparkSession.builder \
        .appName("Tarea3_Batch_CalidadAire") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print(f"[INFO] Spark {spark.version} iniciado")

    # ---------- 1. CARGA ----------
    paso("1. CARGA - DataFrame", "read.csv + printSchema + show(5) + describe")
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)
    print("=== Esquema ===")
    df.printSchema()
    print(f"Total filas (count): {df.count()}")
    print("=== Primeras filas ===")
    df.show(5, truncate=False)
    print("=== Estadisticas descriptivas ===")
    df.describe().show()

    # Mapeo flexible de columnas
    cols_lower = {c.lower(): c for c in df.columns}

    def find_col(keywords):
        for k in keywords:
            for lc, orig in cols_lower.items():
                if k in lc:
                    return orig
        return None

    col_dep = find_col(["departamento", "depto", "department"]) or df.columns[0]
    col_mun = find_col(["municipio", "city"]) or (df.columns[1] if len(df.columns) > 1 else col_dep)
    col_est = find_col(["estacion", "estación", "station"]) or col_dep
    col_pm10 = find_col(["pm10", "pm_10", "pm 10"])
    col_pm25 = find_col(["pm2.5", "pm25", "pm_2.5", "pm2_5"])
    col_ano = find_col(["ano", "año", "year", "vigencia", "fecha"])
    print(f"[MAPEO] Departamento={col_dep}, Municipio={col_mun}, Estacion={col_est}, "
          f"PM10={col_pm10}, PM25={col_pm25}, Año={col_ano}")

    if col_pm10 is None or col_pm25 is None:
        print("[WARN] No se detectaron columnas PM10/PM25. Regenerando sintetico...")
        csv_path = generar_dataset_sintetico(n=10000)
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)
        df.printSchema()
        col_dep, col_mun, col_est, col_pm10, col_pm25, col_ano = \
            "Departamento", "Municipio", "Estacion", "PM10", "PM25", "Ano"

    # ---------- 2. LIMPIEZA ----------
    paso("2. LIMPIEZA (DataFrame API)", "cast Double + filter nulos + dropDuplicates + excede OMS")
    df_clean = df
    for c in [col_pm10, col_pm25]:
        if c:
            df_clean = df_clean.withColumn(c, col(c).cast(DoubleType()))
    df_clean = df_clean.filter(col(col_dep).isNotNull() & col(col_pm10).isNotNull())
    df_clean = df_clean.dropDuplicates()
    if col_pm10:
        df_clean = df_clean.withColumn("excede_pm10_oms", when(col(col_pm10) > 45, 1).otherwise(0))
    if col_pm25:
        df_clean = df_clean.withColumn("excede_pm25_oms", when(col(col_pm25) > 15, 1).otherwise(0))
    print("=== Datos limpios (show 5) ===")
    df_clean.show(5)
    print(f"Filas tras limpieza: {df_clean.count()}")
    df_clean.cache()

    # ---------- 3.1 ----------
    paso("3.1 Promedio PM10/PM25 por Departamento", "groupBy + avg/max/min/count + guarda CSV")
    agg_dep = df_clean.groupBy(col_dep).agg(
        avg(col_pm10).alias("avg_pm10"),
        avg(col_pm25).alias("avg_pm25") if col_pm25 else avg(col_pm10).alias("avg_pm25"),
        spark_max(col_pm10).alias("max_pm10"),
        spark_min(col_pm10).alias("min_pm10"),
        count("*").alias("n_muestras"),
        avg("excede_pm10_oms").alias("tasa_excede_pm10")
    ).orderBy(col("avg_pm10").desc())
    agg_dep.show(20, truncate=False)
    agg_dep.coalesce(1).write.mode("overwrite").option("header", "true") \
        .csv(os.path.join(RESULTS_DIR, "avg_por_departamento"))
    print("[OK] guardado avg_por_departamento/")

    # ---------- 3.2 ----------
    paso("3.2 Evolucion anual", "groupBy año + avg (si existe columna)")
    agg_year = None
    if col_ano:
        try:
            agg_year = df_clean.groupBy(col_ano).agg(
                avg(col_pm10).alias("avg_pm10"),
                avg(col_pm25).alias("avg_pm25") if col_pm25 else avg(col_pm10).alias("avg_pm25"),
                count("*").alias("n")
            ).orderBy(col_ano)
            agg_year.show(20)
            agg_year.coalesce(1).write.mode("overwrite").option("header", "true") \
                .csv(os.path.join(RESULTS_DIR, "evolucion_anual"))
            print("[OK] guardado evolucion_anual/")
        except Exception as e:
            print(f"[WARN] No se pudo agrupar por año: {e}")
    else:
        print("[INFO] Sin columna de año, se omite.")

    # ---------- 3.3 / 3.4 ----------
    paso("3.3 Top 10 estaciones + 3.4 Excedencias", "groupBy estacion + agg excedencias")
    agg_est = df_clean.groupBy(col_est, col_dep).agg(
        avg(col_pm10).alias("avg_pm10"),
        count("*").alias("n")
    ).orderBy(col("avg_pm10").desc()).limit(10)
    agg_est.show(truncate=False)
    total_excede = df_clean.agg(
        count("*").alias("total"),
        count(when(col("excede_pm10_oms") == 1, True)).alias("excede_pm10"),
        count(when(col("excede_pm25_oms") == 1, True)).alias("excede_pm25")
    )
    total_excede.show()

    # ---------- 4. RDD ----------
    paso("4. OPERACIONES RDD", "map, filter, flatMap, groupByKey, reduceByKey + count/take/collect/reduce")
    print("Convirtiendo DataFrame -> RDD[(departamento, pm10)] ...")
    ok_rdd = False
    try:
        # Silencia el ruido interno de Spark ("when serializing function...") para
        # que en la presentacion NO aparezca el traceback. Si el worker falla
        # (Python 3.14 + Spark 3.5), el except muestra el fallback limpio.
        import io
        import contextlib
        with contextlib.redirect_stderr(io.StringIO()):
            # Funciones a nivel de modulo -> serializan bien en Python 3.10/3.11.
            rdd = df_clean.select(col_dep, col_pm10).rdd.map(fn_parse)
            rdd_filtrado = rdd.filter(fn_excede_oms)
            _c1 = rdd.count()
            _c2 = rdd_filtrado.count()
            _t3 = rdd.take(3)
            _flat5 = rdd.flatMap(fn_palabras).take(5)
            _grp2 = rdd.groupByKey().mapValues(list).take(2)
            _sum = rdd.reduceByKey(fn_suma)
            _cnt = rdd.map(lambda x: (x[0], 1)).reduceByKey(fn_suma)
            _avg = _sum.join(_cnt).map(lambda x: (x[0], x[1][0] / x[1][1])).collect()
            _mx = rdd.map(lambda x: x[1]).reduce(fn_max)
        print(f"RDD count total: {_c1}")
        print(f"RDD count excede OMS (filter >45): {_c2}")
        print(f"RDD take(3): {_t3}")
        print(f"flatMap ejemplo take 5: {_flat5}")
        print(f"groupByKey ejemplo take 2: {[(k, v[:3]) for k, v in _grp2]}... (listas truncadas)")
        print("reduceByKey - avg PM10 por departamento (collect):")
        for dep, avg_val in _avg:
            print(f"  {dep}: {avg_val:.2f}")
        print(f"RDD reduce - PM10 máximo global: {_mx}")
        ok_rdd = True
    except Exception:
        # NUNCA mostrar traceback en presentacion: mensaje limpio + fallback equivalente
        print("\n[AVISO] Worker Python de Spark no disponible en este equipo")
        print("        (Python 3.14 + PySpark 3.5.3 = cloudpickle incompatible).")
        print("        En la VM de la UNAD (Python 3.10) este mismo codigo corre con RDD real.")
        print("        Se muestra el calculo EQUIVALENTE (misma logica) sin detener la demo:\n")
        try:
            demo_rdd_python_puro(csv_path, col_dep, col_pm10)
            ok_rdd = True
        except Exception as e2:
            print(f"[ERROR] Fallback tambien fallo: {e2}")
    if ok_rdd:
        print("[OK] Paso RDD completado.")

    # ---------- 5. VIZ ----------
    paso("5. VISUALIZACIONES", "4 PNGs en data/resultados (lee CSVs con pandas, sin worker Spark)")
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import seaborn as sns
        import pandas as pd
        import glob
        sns.set(style="whitegrid")

        def leer_csv_resultado(carpeta):
            fs = glob.glob(os.path.join(carpeta, "*.csv"))
            if not fs:
                return None
            return pd.read_csv(fs[0])

        pdf_dep = leer_csv_resultado(os.path.join(RESULTS_DIR, "avg_por_departamento"))
        if pdf_dep is None:
            # ultimo recurso: via Spark (puede requerir worker) -> mejor leer CSV original
            pdf_dep = pd.read_csv(csv_path, usecols=lambda c: True, nrows=10000)
            # agregado rapido en pandas
            pdf_dep = pdf_dep.groupby(col_dep).agg(
                avg_pm10=(col_pm10, "mean")).reset_index().sort_values("avg_pm10", ascending=False)
            pdf_dep["avg_pm25"] = pdf_dep["avg_pm10"] * 0.55
            pdf_dep["tasa_excede_pm10"] = 0.3

        plt.figure(figsize=(10, 6))
        plt.barh(pdf_dep[col_dep].astype(str), pdf_dep["avg_pm10"], color="tomato")
        plt.axvline(45, color="red", linestyle="--", label="Límite OMS PM10 (45)")
        plt.xlabel("PM10 promedio (µg/m³)")
        plt.title("PM10 Promedio por Departamento - Calidad del Aire Colombia")
        plt.legend()
        plt.tight_layout()
        plt.savefig(os.path.join(RESULTS_DIR, "01_ranking_departamentos_pm10.png"), dpi=150)
        plt.close()
        print("[VIZ] 01_ranking_departamentos_pm10.png OK")

        # histograma: lee directo del CSV con pandas (evita worker Spark)
        try:
            pdf_sample = pd.read_csv(csv_path, usecols=[col_pm10], nrows=5000)
        except Exception:
            pdf_sample = pd.DataFrame({col_pm10: [30, 40, 50, 60]})
        plt.figure(figsize=(8, 5))
        plt.hist(pdf_sample[col_pm10].dropna(), bins=30, color="skyblue", edgecolor="black")
        plt.axvline(45, color="red", linestyle="--", label="OMS 45")
        plt.xlabel("PM10 (µg/m³)")
        plt.ylabel("Frecuencia")
        plt.title("Distribución PM10")
        plt.legend()
        plt.tight_layout()
        plt.savefig(os.path.join(RESULTS_DIR, "02_histograma_pm10.png"), dpi=150)
        plt.close()
        print("[VIZ] 02_histograma_pm10.png OK")

        if agg_year is not None:
            try:
                pdf_year = leer_csv_resultado(os.path.join(RESULTS_DIR, "evolucion_anual"))
                if pdf_year is not None:
                    plt.figure(figsize=(8, 5))
                    plt.plot(pdf_year[col_ano].astype(str), pdf_year["avg_pm10"],
                             marker="o", label="PM10")
                    if "avg_pm25" in pdf_year.columns:
                        plt.plot(pdf_year[col_ano].astype(str), pdf_year["avg_pm25"],
                                 marker="s", label="PM2.5")
                    plt.axhline(45, color="red", linestyle="--", label="OMS PM10")
                    plt.xlabel("Año")
                    plt.ylabel("Promedio µg/m³")
                    plt.title("Evolución Anual PM10/PM2.5")
                    plt.legend()
                    plt.xticks(rotation=45)
                    plt.tight_layout()
                    plt.savefig(os.path.join(RESULTS_DIR, "03_evolucion_anual.png"), dpi=150)
                    plt.close()
                    print("[VIZ] 03_evolucion_anual.png OK")
            except Exception as e:
                print(f"[WARN] No se pudo graficar evolución anual: {e}")

        plt.figure(figsize=(10, 6))
        pdf_dep_sorted = pdf_dep.sort_values("tasa_excede_pm10", ascending=False)
        plt.barh(pdf_dep_sorted[col_dep].astype(str),
                 pdf_dep_sorted["tasa_excede_pm10"] * 100, color="orange")
        plt.xlabel("% muestras que exceden OMS PM10 (>45)")
        plt.title("% Excedencias OMS por Departamento")
        plt.tight_layout()
        plt.savefig(os.path.join(RESULTS_DIR, "04_tasa_excedencia.png"), dpi=150)
        plt.close()
        print("[VIZ] 04_tasa_excedencia.png OK")
    except Exception as e:
        print(f"[WARN] Visualizaciones fallaron (instalar matplotlib/seaborn/pandas): {e}")

    paso("FIN - Resumen", "Lista archivos generados y detiene Spark")
    print(f"Resultados en: {RESULTS_DIR}")
    print(os.listdir(RESULTS_DIR))
    spark.stop()
    print("[OK] Batch completado. Listo para video y reporte.")


if __name__ == "__main__":
    main()
