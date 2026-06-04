# Great Expectations — Calidad de Datos sobre SAP en Databricks

Notebooks y scripts para validar la **calidad de datos de dominios SAP** (Clientes y Proveedores) usando [Great Expectations](https://greatexpectations.io) sobre **Databricks + PySpark**.

El proyecto implementa un pipeline de **gobierno de datos** que carga reglas de negocio desde una tabla Delta, las aplica sobre tablas SAP exportadas y genera resultados de validación consumibles por Databricks Jobs.

## Stack

- **Databricks** (Spark runtime)
- **PySpark** — procesamiento distribuido
- **Great Expectations** — framework de calidad de datos
- **Delta Lake** — tabla de reglas de negocio
- **SAP** — fuente de datos (dominios KNA1-Clientes, LFA1-Proveedores)

## Arquitectura

```
Delta Table (reglas de negocio)
    goviernosdato.regla_negocio_great_expectation
              │
              ▼
  GE Preparar Lista Reglas.py    ← Databricks notebook
  - Filtra reglas por dominio
  - Serializa reglas a JSON
  - Publica a task values (Databricks Jobs)
              │
              ▼
  GE Pruebas Rapidas Final.ipynb ← Notebook de validación
  - Carga tablas SAP (KNA1, LFA1)
  - Aplica expectations de Great Expectations
  - Genera reporte de calidad
```

## Dominios SAP cubiertos

| Dominio | Tabla SAP | Descripción |
|---------|-----------|-------------|
| `DP_CL-Clientes` | KNA1 | Maestro de clientes |
| `DP_PR-Proveedores` | LFA1 / LFB1 | Maestro de proveedores |

## Uso en Databricks

### 1. Preparar lista de reglas

Ejecutar `notebooks/GE Preparar Lista Reglas.py` como notebook o tarea en un Job:

```
Parámetros (widgets):
  rules_table → dev_arqanalitica.gobiernodato.regla_negocio_great_expectation
  dominios    → DP_CL-Clientes,DP_PR-Proveedores
```

El notebook publica `rules_json` y `rules_count` como task values para consumo en tareas downstream.

### 2. Ejecutar validaciones

Abrir `GE Pruebas Rapidas Final.ipynb` en Databricks y ejecutar todas las celdas. Los resultados de Great Expectations incluyen:

- Número de registros evaluados
- Expectations cumplidas / fallidas por columna
- Detalle de registros que violan cada regla

## Datos de muestra

Los archivos `.xlsx` en la raíz son exportaciones de tablas SAP usadas para pruebas locales:

| Archivo | Contenido |
|---------|-----------|
| `KNA1-CLI (1).xlsx` | Muestra de maestro de clientes |
| `LFA1-PROV (1).xlsx` | Muestra de maestro de proveedores |
| `LFA1.XLSX` / `LFB1.xlsx` | Datos adicionales de proveedores |

## Requisitos

- Databricks Runtime con PySpark
- Permisos de lectura sobre la tabla `goviernosdato.regla_negocio_great_expectation`
- Great Expectations instalado en el cluster (`%pip install great-expectations`)
