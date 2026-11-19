"""
=============================================================================
Taller local: Data Engineer Junior Advanced
Pipeline ETL de Ventas con PySpark y Spark SQL
=============================================================================
Autor: Senior Data Engineer
Tecnologías: Python 3.11+, PySpark 3.5+, Spark SQL, Parquet
=============================================================================
"""

import os
import sys
import zipfile
import re
import csv
import json
from decimal import Decimal

# ---------------------------------------------------------------------------
# 0. CONFIGURACIÓN DEL ENTORNO LOCAL (WINDOWS / UNIX)
# ---------------------------------------------------------------------------
def configure_environment():
    """Configura variables de entorno para ejecución local sin fallos en Windows."""
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    # Si existe una carpeta local 'hadoop' con winutils, configurarla
    hadoop_home = os.path.abspath('hadoop')
    if os.path.exists(os.path.join(hadoop_home, 'bin', 'winutils.exe')):
        os.environ['HADOOP_HOME'] = hadoop_home
        os.environ['hadoop.home.dir'] = hadoop_home
        os.environ['PATH'] = os.path.join(hadoop_home, 'bin') + ';' + os.environ.get('PATH', '')


def extract_excel_if_needed(excel_path="taller_data_engineer.xlsx", target_dir="datos"):
    """
    Extrae las 6 hojas de taller_data_engineer.xlsx a archivos CSV y JSONL en datos/
    si no existen previamente, garantizando reproducibilidad sin dependencias externas.
    """
    if os.path.exists(os.path.join(target_dir, "orders.csv")):
        print(f"[PRE-CHECK] Archivos fuente ya presentes en '{target_dir}/'. Omitiendo extracción.")
        return

    if not os.path.exists(excel_path):
        raise FileNotFoundError(f"No se encontró el archivo Excel base: {excel_path}")

    print(f"[ETL SETUP] Extrayendo hojas de '{excel_path}' hacia '{target_dir}/'...")
    import xml.etree.ElementTree as ET

    def col2idx(col_str):
        idx = 0
        for char in col_str:
            idx = idx * 26 + (ord(char) - ord('A') + 1)
        return idx - 1

    def parse_sheet(xml_bytes):
        root = ET.fromstring(xml_bytes)
        rows_data = []
        ns = '{http://schemas.openxmlformats.org/spreadsheetml/2006/main}'
        for r in root.findall(f'{ns}sheetData/{ns}row'):
            row_dict = {}
            max_col = 0
            for c in r.findall(f'{ns}c'):
                cell_ref = c.attrib.get('r', '')
                match = re.match(r'([A-Z]+)', cell_ref)
                if not match:
                    continue
                col_idx = col2idx(match.group(1))
                t = c.find(f'{ns}is/{ns}t')
                v = c.find(f'{ns}v')
                val = ''
                if t is not None and t.text:
                    val = t.text
                elif v is not None and v.text:
                    val = v.text
                row_dict[col_idx] = val
                if col_idx > max_col:
                    max_col = col_idx
            rows_data.append(row_dict)

        header = [rows_data[0].get(i, '') for i in range(max(rows_data[0].keys()) + 1)]
        num_cols = len(header)
        table = [header]
        for r in rows_data[1:]:
            table.append([r.get(i, '') for i in range(num_cols)])
        return table

    os.makedirs(os.path.join(target_dir, "incremental"), exist_ok=True)

    with zipfile.ZipFile(excel_path) as z:
        # sheet1: clientes -> customers.csv
        cust_data = parse_sheet(z.read('xl/worksheets/sheet1.xml'))
        with open(os.path.join(target_dir, 'customers.csv'), 'w', newline='', encoding='utf-8') as f:
            csv.writer(f).writerows(cust_data)

        # sheet2: productos -> products.csv
        prod_data = parse_sheet(z.read('xl/worksheets/sheet2.xml'))
        with open(os.path.join(target_dir, 'products.csv'), 'w', newline='', encoding='utf-8') as f:
            csv.writer(f).writerows(prod_data)

        # sheet3: pedidos -> orders.csv
        ord_data = parse_sheet(z.read('xl/worksheets/sheet3.xml'))
        with open(os.path.join(target_dir, 'orders.csv'), 'w', newline='', encoding='utf-8') as f:
            csv.writer(f).writerows(ord_data)

        # sheet4: detalle_pedidos -> order_items.csv
        items_data = parse_sheet(z.read('xl/worksheets/sheet4.xml'))
        with open(os.path.join(target_dir, 'order_items.csv'), 'w', newline='', encoding='utf-8') as f:
            csv.writer(f).writerows(items_data)

        # sheet5: pedidos_incrementales -> orders_incremental.jsonl
        ord_inc_data = parse_sheet(z.read('xl/worksheets/sheet5.xml'))
        inc_hdr = ord_inc_data[0]
        with open(os.path.join(target_dir, 'incremental', 'orders_incremental.jsonl'), 'w', encoding='utf-8') as f:
            for row in ord_inc_data[1:]:
                f.write(json.dumps(dict(zip(inc_hdr, row)), ensure_ascii=False) + '\n')

        # sheet6: detalle_incremental -> order_items_incremental.jsonl
        item_inc_data = parse_sheet(z.read('xl/worksheets/sheet6.xml'))
        inc_item_hdr = item_inc_data[0]
        with open(os.path.join(target_dir, 'incremental', 'order_items_incremental.jsonl'), 'w', encoding='utf-8') as f:
            for row in item_inc_data[1:]:
                f.write(json.dumps(dict(zip(inc_item_hdr, row)), ensure_ascii=False) + '\n')

    print("[ETL SETUP] Extracción completada exitosamente en datos/.")


# ---------------------------------------------------------------------------
# PIPELINE PRINCIPAL EN PYSPARK
# ---------------------------------------------------------------------------
def run_pipeline():
    configure_environment()
    extract_excel_if_needed()

    from pyspark.sql import SparkSession, Window
    from pyspark.sql.functions import (
        col, when, to_date, to_timestamp, round as spark_round,
        sum as spark_sum, count, countDistinct, date_format,
        row_number, lit, broadcast, coalesce
    )
    from pyspark.sql.types import (
        StringType, IntegerType, DoubleType, BooleanType, DecimalType
    )

    print("\n" + "="*80)
    print("FASE 1: INICIALIZACIÓN Y LECTURA LOCAL CON CONVERSIÓN EXPLÍCITA")
    print("="*80)

    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("taller-data-engineer")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print(f"[*] SparkSession creada exitosamente. Versión: {spark.version}")

    # Lectura de los cuatro archivos CSV con inferSchema=False y header=True
    raw_customers = spark.read.option("header", True).option("inferSchema", False).csv("datos/customers.csv")
    raw_products = spark.read.option("header", True).option("inferSchema", False).csv("datos/products.csv")
    raw_orders = spark.read.option("header", True).option("inferSchema", False).csv("datos/orders.csv")
    raw_order_items = spark.read.option("header", True).option("inferSchema", False).csv("datos/order_items.csv")

    print(f"[*] Registros leídos de CSV crudos:")
    print(f"    - Customers:   {raw_customers.count()} filas")
    print(f"    - Products:    {raw_products.count()} filas")
    print(f"    - Orders:      {raw_orders.count()} filas")
    print(f"    - Order Items: {raw_order_items.count()} filas")

    # Conversión explícita de tipos de datos antes de operar
    customers_typed = raw_customers.select(
        col("customer_id").cast(StringType()),
        col("first_name").cast(StringType()),
        col("last_name").cast(StringType()),
        col("email").cast(StringType()),
        col("country").cast(StringType()),
        to_date(col("signup_date"), "yyyy-MM-dd").alias("signup_date"),
        col("segment").cast(StringType())
    )

    products_typed = raw_products.select(
        col("product_id").cast(StringType()),
        col("product_name").cast(StringType()),
        col("category").cast(StringType()),
        col("unit_price").cast(DoubleType()),
        col("active").cast(BooleanType())
    )

    orders_typed = raw_orders.select(
        col("order_id").cast(StringType()),
        col("customer_id").cast(StringType()),
        to_timestamp(col("order_ts")).alias("order_ts"),
        col("status").cast(StringType()),
        col("channel").cast(StringType()),
        to_timestamp(col("updated_at")).alias("updated_at")
    )

    order_items_typed = raw_order_items.select(
        col("line_id").cast(StringType()),
        col("order_id").cast(StringType()),
        col("product_id").cast(StringType()),
        col("quantity").cast(IntegerType()),
        col("discount_pct").cast(DoubleType())
    )

    print("\n[+] Esquema explícito de 'orders':")
    orders_typed.printSchema()
    print("[+] Muestra de 'order_items':")
    order_items_typed.show(3, truncate=False)

    print("\n" + "="*80)
    print("FASE 2: PERFILAMIENTO, DIAGNÓSTICO DE CALIDAD Y TRATAMIENTO DE ANOMALÍAS")
    print("="*80)

    # -----------------------------------------------------------------------
    # ANOMALÍA 1: DUPLICADOS EN CLAVES PRIMARIAS (customers y orders)
    # Decisión: CORREGIR (Deduplicación canónica)
    # Justificación: Duplicados idénticos por reintentos de ingestión. Se conserva
    # una sola versión verdadera sin descartar la entidad.
    # -----------------------------------------------------------------------
    w_cust = Window.partitionBy("customer_id").orderBy(col("signup_date").desc())
    clean_customers = (
        customers_typed
        .withColumn("rn", row_number().over(w_cust))
        .filter(col("rn") == 1)
        .drop("rn")
    )
    cust_dups_count = customers_typed.count() - clean_customers.count()
    print(f"[Anomalía 1 - Duplicados Customers] {cust_dups_count} fila(s) duplicada(s) corregida(s). Total limpio: {clean_customers.count()}")

    w_ord = Window.partitionBy("order_id").orderBy(col("updated_at").desc())
    dedup_orders = (
        orders_typed
        .withColumn("rn", row_number().over(w_ord))
        .filter(col("rn") == 1)
        .drop("rn")
    )
    ord_dups_count = orders_typed.count() - dedup_orders.count()
    print(f"[Anomalía 1 - Duplicados Orders]    {ord_dups_count} fila(s) duplicada(s) corregida(s). Total único: {dedup_orders.count()}")

    # -----------------------------------------------------------------------
    # ANOMALÍA 2: ERROR TIPOGRÁFICO EN STATUS ('deliverd' -> 'delivered')
    # Decisión: CORREGIR (Normalización determinística de dominio)
    # Justificación: Error humano/sistémico menor. La intención es indudable.
    # -----------------------------------------------------------------------
    corrected_orders = dedup_orders.withColumn(
        "status",
        when(col("status") == "deliverd", "delivered").otherwise(col("status"))
    )
    typo_count = dedup_orders.filter(col("status") == "deliverd").count()
    print(f"[Anomalía 2 - Typo en Status]      {typo_count} fila(s) con 'deliverd' estandarizada(s) a 'delivered'.")

    # -----------------------------------------------------------------------
    # ANOMALÍA 3: FECHAS/TIMESTAMPS MAL FORMADOS EN PEDIDOS ('2026-99-31T12:00:00Z')
    # Decisión: ENVIAR A CUARENTENA
    # Justificación: Una fecha imposible no permite ubicar el pedido en la dimensión
    # temporal del negocio ni imputar un mes contable.
    # -----------------------------------------------------------------------
    quarantine_malformed_ts_orders = corrected_orders.filter(col("order_ts").isNull()).withColumn(
        "quarantine_reason", lit("MALFORMED_ORDER_TIMESTAMP")
    )
    valid_ts_orders = corrected_orders.filter(col("order_ts").isNotNull())
    print(f"[Anomalía 3 - Timestamp Inválido]  {quarantine_malformed_ts_orders.count()} pedido(s) enviado(s) a cuarentena por fecha mal formada.")

    # -----------------------------------------------------------------------
    # ANOMALÍA 4: REFERENCIAS CRUZADAS HUÉRFANAS / NULAS (Integridad Referencial)
    # Decisión: ENVIAR A CUARENTENA CON AUDITORÍA
    # Justificación: No se descartan silenciosamente para trazabilidad financiera,
    # pero no pueden alimentar el modelo analítico dimensional.
    # -----------------------------------------------------------------------
    orders_with_cust = valid_ts_orders.join(
        broadcast(clean_customers.select("customer_id").withColumn("cust_exists", lit(True))),
        on="customer_id",
        how="left"
    )
    quarantine_orphan_orders = (
        orders_with_cust
        .filter(col("cust_exists").isNull())
        .withColumn("quarantine_reason", when(col("customer_id").isNull() | (col("customer_id") == ""), "NULL_CUSTOMER").otherwise("ORPHAN_CUSTOMER"))
        .drop("cust_exists")
    )
    clean_orders = orders_with_cust.filter(col("cust_exists").isNotNull()).drop("cust_exists")
    print(f"[Anomalía 4 - Integridad Órdenes]  {quarantine_orphan_orders.count()} pedido(s) enviado(s) a cuarentena por cliente inexistente o nulo.")

    # Catálogo de Productos: Precios negativos o vacíos
    quarantine_products = products_typed.filter(col("unit_price").isNull() | (col("unit_price") <= 0)).withColumn(
        "quarantine_reason", when(col("unit_price").isNull(), "NULL_PRICE").otherwise("NEGATIVE_PRICE")
    )
    clean_products = products_typed.filter(col("unit_price").isNotNull() & (col("unit_price") > 0))
    print(f"[Anomalía 4 - Productos Inválidos] {quarantine_products.count()} producto(s) en cuarentena por precio nulo/negativo. Limpios: {clean_products.count()}")

    # -----------------------------------------------------------------------
    # ANOMALÍA 5: CANTIDADES FUERA DE RANGO Y DESCUENTOS INVÁLIDOS EN DETALLE
    # Decisión: RECHAZAR / CUARENTENA DE LÍNEAS
    # Justificación: quantity <= 0 o discount_pct > 1.0 (120%) violan leyes
    # aritméticas del negocio produciendo ingresos netos negativos irreales.
    # -----------------------------------------------------------------------
    rejected_items = order_items_typed.filter(
        col("quantity").isNull() | (col("quantity") <= 0) |
        col("discount_pct").isNull() | (col("discount_pct") < 0.0) | (col("discount_pct") > 1.0)
    ).withColumn(
        "rejection_reason",
        when(col("quantity").isNull() | (col("quantity") <= 0), "INVALID_QUANTITY")
        .otherwise("INVALID_DISCOUNT_PCT")
    )
    valid_domain_items = order_items_typed.filter(
        col("quantity").isNotNull() & (col("quantity") > 0) &
        col("discount_pct").isNotNull() & (col("discount_pct") >= 0.0) & (col("discount_pct") <= 1.0)
    )
    print(f"[Anomalía 5 - Dominio de Ítems]     {rejected_items.count()} línea(s) rechazada(s) por cantidad <= 0 o descuento fuera de rango [0, 1].")

    # Integridad referencial de order_items con clean_orders y clean_products
    items_fk = (
        valid_domain_items
        .join(broadcast(clean_orders.select("order_id").withColumn("ord_exists", lit(True))), on="order_id", how="left")
        .join(broadcast(clean_products.select("product_id").withColumn("prod_exists", lit(True))), on="product_id", how="left")
    )
    quarantine_items = (
        items_fk
        .filter(col("ord_exists").isNull() | col("prod_exists").isNull())
        .withColumn("quarantine_reason",
            when(col("ord_exists").isNull() & col("prod_exists").isNull(), "ORPHAN_ORDER_AND_PRODUCT")
            .when(col("ord_exists").isNull(), "ORPHAN_ORDER_OR_PARENT_IN_QUARANTINE")
            .otherwise("ORPHAN_PRODUCT")
        )
        .drop("ord_exists", "prod_exists")
    )
    clean_order_items = items_fk.filter(col("ord_exists").isNotNull() & col("prod_exists").isNotNull()).drop("ord_exists", "prod_exists")
    print(f"[Anomalía 4 - Integridad de Ítems] {quarantine_items.count()} línea(s) en cuarentena por FK huérfana o pedido padre en cuarentena.")
    print(f"[*] Total de líneas de pedido listas y limpias: {clean_order_items.count()}")

    print("\n" + "="*80)
    print("FASE 3: LIMPIEZA, JOINS Y CÁLCULO DEL IMPORTE NETO")
    print("="*80)

    # Granularidad: Asegurar que el join no duplique registros preservando line_id
    sales_detail = (
        clean_order_items
        .join(clean_orders.select("order_id", "customer_id", "order_ts", "status", "channel"), on="order_id", how="inner")
        .join(broadcast(clean_customers.select("customer_id", "country", "segment")), on="customer_id", how="inner")
        .join(broadcast(clean_products.select("product_id", "product_name", "category", "unit_price")), on="product_id", how="inner")
        .withColumn(
            "net_amount",
            spark_round(col("quantity") * col("unit_price") * (lit(1.0) - col("discount_pct")), 2)
        )
    )

    print(f"[*] Registros resultantes en detalle de ventas: {sales_detail.count()} (granularidad 1:1 con line_id)")
    sales_detail.select(
        "line_id", "order_id", "country", "channel", "category",
        "quantity", "unit_price", "discount_pct", "net_amount"
    ).show(5, truncate=False)

    print("\n" + "="*80)
    print("FASE 4: AGREGACIONES DE NEGOCIO (MES, PAÍS, CANAL, CATEGORÍA)")
    print("="*80)

    # 1. API de DataFrames
    monthly_sales_df = (
        sales_detail
        .withColumn("order_month", date_format(col("order_ts"), "yyyy-MM"))
        .groupBy("order_month", "country", "channel", "category")
        .agg(
            spark_round(spark_sum("net_amount"), 2).alias("total_net_amount"),
            spark_sum("quantity").alias("total_units"),
            countDistinct("order_id").alias("unique_orders"),
            count("line_id").alias("total_lines")
        )
        .orderBy("order_month", "country", "channel", "category")
    )

    print("[*] Resumen mensual agregado con DataFrame API:")
    monthly_sales_df.show(8, truncate=False)

    # 2. Spark SQL (validación alternativa)
    sales_detail.createOrReplaceTempView("v_sales_detail")
    monthly_sales_sql = spark.sql("""
        SELECT 
            date_format(order_ts, 'yyyy-MM') AS order_month,
            country,
            channel,
            category,
            round(sum(net_amount), 2) AS total_net_amount,
            sum(quantity) AS total_units,
            count(distinct order_id) AS unique_orders,
            count(line_id) AS total_lines
        FROM v_sales_detail
        GROUP BY 1, 2, 3, 4
        ORDER BY 1, 2, 3, 4
    """)

    print("\n" + "="*80)
    print("FASE 5: RETO INCREMENTAL IDEMPOTENTE")
    print("="*80)

    raw_inc_orders = spark.read.json("datos/incremental/orders_incremental.jsonl")
    raw_inc_items = spark.read.json("datos/incremental/order_items_incremental.jsonl")

    inc_orders_typed = raw_inc_orders.select(
        col("order_id").cast(StringType()),
        col("customer_id").cast(StringType()),
        to_timestamp(col("order_ts")).alias("order_ts"),
        when(col("status") == "deliverd", "delivered").otherwise(col("status")).alias("status"),
        col("channel").cast(StringType()),
        to_timestamp(col("updated_at")).alias("updated_at")
    )

    inc_items_typed = raw_inc_items.select(
        col("line_id").cast(StringType()),
        col("order_id").cast(StringType()),
        col("product_id").cast(StringType()),
        col("quantity").cast(IntegerType()),
        col("discount_pct").cast(DoubleType())
    )

    print(f"[*] Registros en lote incremental:")
    print(f"    - Nuevos/actualizados Orders: {inc_orders_typed.count()}")
    print(f"    - Nuevos Order Items:        {inc_items_typed.count()}")

    # Estrategia Idempotente: Unir histórico + incremental y aplicar ventana por order_id ordenado por updated_at DESC
    combined_orders = orders_typed.unionByName(inc_orders_typed)
    w_merge_orders = Window.partitionBy("order_id").orderBy(col("updated_at").desc())

    idempotent_orders = (
        combined_orders
        .withColumn("rn", row_number().over(w_merge_orders))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    # Para order_items, deduplicar por line_id
    combined_items = order_items_typed.unionByName(inc_items_typed)
    w_merge_items = Window.partitionBy("line_id").orderBy(col("line_id"))
    idempotent_items = (
        combined_items
        .withColumn("rn", row_number().over(w_merge_items))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    print(f"[*] Pedidos totales tras merge idempotente: {idempotent_orders.count()} (20.000 únicos + 100 nuevos = 20.100)")
    print(f"[*] Líneas totales tras merge idempotente:  {idempotent_items.count()} (40.026 únicas + 200 nuevas = 40.226)")

    # Validación de precedencia: inspeccionar el pedido O00005 actualizado
    sample_updated = idempotent_orders.filter(col("order_id") == "O00005").collect()[0]
    print(f"[+] Verificación pedido O00005 actualizado:")
    print(f"    - updated_at conservado: {sample_updated['updated_at']} (debe ser la fecha tardía 2026-03-05)")

    # Prueba de Idempotencia: Reprocesar el mismo lote sobre el resultado previo
    re_orders = (
        idempotent_orders.unionByName(inc_orders_typed)
        .withColumn("rn", row_number().over(w_merge_orders))
        .filter(col("rn") == 1)
        .drop("rn")
    )
    print(f"[+] Prueba de reprocesamiento idempotente: {re_orders.count()} pedidos (invariante)")
    assert re_orders.count() == idempotent_orders.count(), "Fallo en la prueba de idempotencia"

    print("\n" + "="*80)
    print("FASE 6: PERSISTENCIA Y CONTROLES DE CALIDAD (QA)")
    print("="*80)

    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(os.path.join(output_dir, "quarantine"), exist_ok=True)

    # Persistir tablas limpias y agregadas en formato Parquet
    sales_detail_path = os.path.join(output_dir, "sales_detail.parquet")
    monthly_sales_path = os.path.join(output_dir, "monthly_sales.parquet")
    sales_detail.write.mode("overwrite").parquet(sales_detail_path)
    monthly_sales_df.write.mode("overwrite").parquet(monthly_sales_path)

    # Persistir tablas de cuarentena y rechazos para auditoría
    quarantine_orphan_orders.write.mode("overwrite").parquet(os.path.join(output_dir, "quarantine", "orphan_orders.parquet"))
    quarantine_malformed_ts_orders.write.mode("overwrite").parquet(os.path.join(output_dir, "quarantine", "malformed_ts_orders.parquet"))
    quarantine_items.write.mode("overwrite").parquet(os.path.join(output_dir, "quarantine", "orphan_order_items.parquet"))
    rejected_items.write.mode("overwrite").parquet(os.path.join(output_dir, "quarantine", "rejected_order_items.parquet"))

    print(f"[*] Salida persistida con éxito en '{output_dir}/' en formato Parquet.")

    # -----------------------------------------------------------------------
    # CONTROL DE CALIDAD 1: RECONCILIACIÓN TOTAL DE IMPORTES (Detalle vs Agregado)
    # -----------------------------------------------------------------------
    total_detail = sales_detail.agg(spark_sum("net_amount")).collect()[0][0]
    total_agg = monthly_sales_df.agg(spark_sum("total_net_amount")).collect()[0][0]
    diff = abs(total_detail - total_agg)

    print(f"\n[CONTROL QA 1] Reconciliación de Importe Neto:")
    print(f"    - Suma detalle (sales_detail):  ${total_detail:,.2f}")
    print(f"    - Suma agregado (monthly_sales): ${total_agg:,.2f}")
    print(f"    - Diferencia absoluta:          ${diff:.4f}")
    assert diff < 0.01, f"Fallo en reconciliación de totales. Diferencia={diff}"
    print("    => RESULTADO: PASÓ (100% Reconciliado)")

    # -----------------------------------------------------------------------
    # CONTROL DE CALIDAD 2: UNICIDAD Y NO DUPLICACIÓN DE CLAVES PRIMARIAS
    # -----------------------------------------------------------------------
    total_rows = sales_detail.count()
    distinct_lines = sales_detail.select(countDistinct("line_id")).collect()[0][0]
    print(f"\n[CONTROL QA 2] Unicidad de Clave Primaria en Join:")
    print(f"    - Total filas en sales_detail:   {total_rows}")
    print(f"    - Distinct line_id en resultado: {distinct_lines}")
    assert total_rows == distinct_lines, "Se encontraron line_id duplicados en el join"
    print("    => RESULTADO: PASÓ (Granularidad 1:1 exacta, sin multiplicación de importes)")

    # -----------------------------------------------------------------------
    # CONTROL DE CALIDAD 3: AUDITORÍA DE BALANCE DE FILAS (Row Conservation QA)
    # -----------------------------------------------------------------------
    raw_item_count = raw_order_items.count()
    clean_item_count = clean_order_items.count()
    rej_item_count = rejected_items.count()
    quar_item_count = quarantine_items.count()

    print(f"\n[CONTROL QA 3] Balance Total de Filas (order_items):")
    print(f"    - Total inicial crudo:       {raw_item_count}")
    print(f"    - Limpios procesados:        {clean_item_count}")
    print(f"    - Rechazados (dominio):      {rej_item_count}")
    print(f"    - En cuarentena (huérfanos): {quar_item_count}")
    print(f"    - Suma contable (L+R+C):     {clean_item_count + rej_item_count + quar_item_count}")
    assert raw_item_count == (clean_item_count + rej_item_count + quar_item_count), "Discrepancia en balance de filas"
    print("    => RESULTADO: PASÓ (Cero filas descartadas silenciosamente)")

    print("\n" + "="*80)
    print("PIPELINE EJECUTADO Y VALIDADO CON ÉXITO")
    print("="*80)

    spark.stop()


if __name__ == "__main__":
    run_pipeline()
