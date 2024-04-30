# Databricks notebook source
# mount if not exists
containersList = ["finance"]
for containerName in containersList:
    mount_point = f'/mnt/{containerName}'
    is_mounted_bool = any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts())
    if not is_mounted_bool:
        storageAccountName = "storageaccdavid"
        sas = "sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupiytfx&se=2024-05-03T23:29:40Z&st=2024-04-25T15:29:40Z&spr=https&sig=WkQbFULtVS2vzdeXaqw4z0UZWLmU%2BFLh2ZHhQFN30cw%3D" #last until May 3th

        url = "wasbs://" + containerName + "@" + storageAccountName + ".blob.core.windows.net/"
        config = "fs.azure.sas." + containerName + "." + storageAccountName + ".blob.core.windows.net"
        dbutils.fs.mount(
            source = url,
            mount_point= mount_point,
            extra_configs= {config: sas}
        ) 

# COMMAND ----------

import pandas as pd

# COMMAND ----------

dbutils.fs.ls("mnt/finance")

# COMMAND ----------

excel_path = f"../mnt/finance/david_finance.xlsx"

# COMMAND ----------

df_pandas = pd.read_excel(excel_path, engine = "openpyxl", sheet_name = "nueva_deuda_a_plazos")

# COMMAND ----------

fecha_de_transaccion = pd.to_datetime(fecha_de_transaccion_str, format="%Y-%m-%d")
fecha_de_inicio = pd.to_datetime(fecha_de_inicio_str, format="%Y-%m-%d")
numero_de_plazos = 12 
dias_de_margen = 0 # solo aplica para semanal, catorcenal, quincenal
mes_de_cobro = fecha_de_inicio.month - 1
dia_de_cobro = fecha_de_inicio.day - 1

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS finance

# COMMAND ----------

# MAGIC %sql
# MAGIC USE finance

# COMMAND ----------

sparl.sql(f"""
          CREATE TABLE IF NOT EXISTS hist_transacciones_a_cargo (
              fecha_de_cargo DATE,
              descripcion STRING,
              categoria STRING,
              monto DOUBLE,
              forma_de_pago STRING,
              realizado_por STRING,
              limite_de_pago DATE,
              deuda_a_plazos_recurrente_o_normal STRING,
              comentarios STRING
          )
            USING DELTA
            OPTIONS (path 'dbfs:/user/hive/warehouse/finance.db/hist_transacciones_a_cargo')
            TBLPROPERTIES (delta.enableChangeDataFeed = true)
          """)

# COMMAND ----------

formas_de_pago_dict = {
    "tarjetas_de_credito":{
        "LikeU": {
            "dia_de_corte": 12,
            "dia_de_pago": 1
        },
        "Hey Credito": {
            "dia_de_corte": 12,
            "dia_de_pago": 1
        },
        "Liverpool": {
            "dia_de_corte": 19,
            "dia_de_pago": 19
        },
        "Costco": {
            "dia_de_corte": 15,
            "dia_de_pago": 5
        },
        "Fiesta Rewards": {
            "dia_de_corte": 6,
            "dia_de_pago": 27
        }
        
    },
    "debito_y_cuentas": ["Hey Debito", "Santander Universidades", "Banamex Mi Cuenta", "Priority", "Efectivo"]
}

# COMMAND ----------

# Read csv input file
dfp = spark.read.format("csv").option("delimiter",",").option("header",True).option("inferSchema",True).load('/mnt/finance/nueva_deuda_a_plazos.csv')

numero_de_plazos = list(dfp.select("numero_de_plazos"))


# Dias y meses de cobro con base en numero_de_plazos
days_months = [{'dia': dia_de_cobro, 'mes': mes_de_cobro} for dia_de_cobro, mes_de_cobro in numero_de_plazos, dias_de_cobro_list, meses_de_cobro_list]

