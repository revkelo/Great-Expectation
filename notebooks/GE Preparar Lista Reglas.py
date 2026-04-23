# Databricks notebook source
import json
from pyspark.sql import functions as F

DEFAULT_RULES_TABLE = "dev_arqanalitica.gobiernodato.regla_negocio_great_expectation"
DEFAULT_DOMINIOS = "DP_CL-Clientes,DP_PR-Proveedores"

if 'spark' not in globals() or 'dbutils' not in globals():
    raise RuntimeError("Este notebook debe ejecutarse en Databricks (spark/dbutils).")

dbutils.widgets.text("rules_table", DEFAULT_RULES_TABLE)
dbutils.widgets.text("dominios", DEFAULT_DOMINIOS)

rules_table = dbutils.widgets.get("rules_table").strip() or DEFAULT_RULES_TABLE
dominios_raw = dbutils.widgets.get("dominios").strip() or DEFAULT_DOMINIOS
dominios = [d.strip() for d in dominios_raw.split(",") if d.strip()]

if not dominios:
    raise ValueError("Debes informar al menos un dominio en el widget 'dominios'.")

rules_df = (
    spark.table(rules_table)
    .where(F.col("dominio").isin(dominios))
    .select(
        F.trim(F.col("Pkregla").cast("string")).alias("pkregla"),
        F.col("dominio"),
        F.trim(F.col("path").cast("string")).alias("resource"),
    )
    .where(F.col("pkregla") != "")
    .dropDuplicates(["pkregla"])
)

rules_pdf = rules_df.toPandas()
if rules_pdf.empty:
    rules = []
else:
    rules_pdf["pk_num"] = (
        rules_pdf["pkregla"].str.extract(r"^(\\d+)$", expand=False).astype("float")
    )
    rules_pdf = rules_pdf.sort_values(["pk_num", "pkregla"], na_position="last")
    rules = [
        {
            "pkregla": str(r.pkregla),
            "dominio": str(r.dominio),
            "resource": "" if r.resource is None else str(r.resource),
        }
        for r in rules_pdf.itertuples(index=False)
    ]

rules_json = json.dumps(rules, ensure_ascii=False)

# Para consumo en Jobs (for_each_task.inputs)
dbutils.jobs.taskValues.set(key="rules_json", value=rules_json)
dbutils.jobs.taskValues.set(key="rules_count", value=str(len(rules)))

print({
    "rules_table": rules_table,
    "dominios": dominios,
    "rules_count": len(rules),
    "task_value_key": "rules_json"
})
print(rules_json)
