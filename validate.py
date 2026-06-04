"""
SAP Data Quality Validation — Great Expectations (standalone)
Valida los dominios KNA1 (Clientes) y LFA1 (Proveedores) usando los
archivos Excel de muestra incluidos en el repo.

Uso:
    pip install -r requirements.txt
    python validate.py
"""

import json
import sys
from pathlib import Path
import pandas as pd
import great_expectations as gx
from great_expectations.core import ExpectationSuite
from great_expectations.dataset import PandasDataset

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------
DATA_FILES = {
    "KNA1_Clientes":    "KNA1-CLI (1).xlsx",
    "LFA1_Proveedores": "LFA1-PROV (1).xlsx",
}

# ---------------------------------------------------------------------------
# Reglas de calidad por dominio
# Cada regla es (método GE, kwargs)
# ---------------------------------------------------------------------------
RULES = {
    "KNA1_Clientes": [
        # Clave primaria: no nula y única
        ("expect_column_values_to_not_be_null",      {"column": "KUNNR"}),
        ("expect_column_values_to_be_unique",         {"column": "KUNNR"}),
        # Nombre del cliente es obligatorio
        ("expect_column_values_to_not_be_null",      {"column": "NAME1"}),
        # País: código ISO de 2 letras, no nulo
        ("expect_column_values_to_not_be_null",      {"column": "LAND1"}),
        ("expect_column_values_to_match_regex",       {"column": "LAND1", "regex": r"^[A-Z]{2}$"}),
        # Mandante SAP: no nulo
        ("expect_column_values_to_not_be_null",      {"column": "MANDT"}),
        # Ciudad registrada
        ("expect_column_values_to_not_be_null",      {"column": "ORT01", "mostly": 0.80}),
        # Flag de borrado lógico debe ser vacío o "X"
        ("expect_column_values_to_be_in_set",         {"column": "LOEVM", "value_set": ["", "X", None]}),
        # El dataset tiene registros
        ("expect_table_row_count_to_be_between",      {"min_value": 1, "max_value": 500_000}),
        # Columnas esperadas presentes
        ("expect_table_columns_to_match_set",         {
            "column_set": ["MANDT", "KUNNR", "LAND1", "NAME1", "ORT01", "PSTLZ"],
            "exact_match": False,
        }),
    ],
    "LFA1_Proveedores": [
        # Clave primaria: no nula y única
        ("expect_column_values_to_not_be_null",      {"column": "LIFNR"}),
        ("expect_column_values_to_be_unique",         {"column": "LIFNR"}),
        # Nombre del proveedor es obligatorio
        ("expect_column_values_to_not_be_null",      {"column": "NAME1"}),
        # País: código ISO de 2 letras, no nulo
        ("expect_column_values_to_not_be_null",      {"column": "LAND1"}),
        ("expect_column_values_to_match_regex",       {"column": "LAND1", "regex": r"^[A-Z]{2}$"}),
        # Mandante SAP: no nulo
        ("expect_column_values_to_not_be_null",      {"column": "MANDT"}),
        # Ciudad registrada
        ("expect_column_values_to_not_be_null",      {"column": "ORT01", "mostly": 0.80}),
        # Flag de borrado lógico
        ("expect_column_values_to_be_in_set",         {"column": "LOEVM", "value_set": ["", "X", None]}),
        # El dataset tiene registros
        ("expect_table_row_count_to_be_between",      {"min_value": 1, "max_value": 500_000}),
        # Columnas esperadas presentes
        ("expect_table_columns_to_match_set",         {
            "column_set": ["MANDT", "LIFNR", "LAND1", "NAME1", "ORT01", "PSTLZ"],
            "exact_match": False,
        }),
    ],
}

# ---------------------------------------------------------------------------
# Validación
# ---------------------------------------------------------------------------
def validate_domain(name: str, filepath: str, rules: list) -> dict:
    path = Path(filepath)
    if not path.exists():
        print(f"  [SKIP] Archivo no encontrado: {filepath}")
        return {"domain": name, "skipped": True}

    df = pd.read_excel(path)
    print(f"\n{'='*60}")
    print(f"  Dominio : {name}")
    print(f"  Archivo : {filepath}")
    print(f"  Filas   : {len(df):,}  |  Columnas: {len(df.columns)}")
    print(f"{'='*60}")

    dataset = PandasDataset(df)
    results = []
    passed = 0

    for method, kwargs in rules:
        result = getattr(dataset, method)(**kwargs)
        ok = result["success"]
        passed += int(ok)

        # Estadísticas de la regla
        stats = result.get("result", {})
        unexpected = stats.get("unexpected_count") or 0
        total      = stats.get("element_count") or len(df)
        pct        = stats.get("unexpected_percent") or 0.0

        status = "PASS" if ok else "FAIL"
        col    = kwargs.get("column", "-")
        label  = f"{method.replace('expect_', '')} [{col}]"

        if ok:
            print(f"  [{status}] {label}")
        else:
            print(f"  [{status}] {label}  <- {unexpected}/{total} filas invalidas ({pct:.1f}%)")

        results.append({
            "rule":    method,
            "column":  col,
            "passed":  ok,
            "unexpected_count": unexpected,
            "unexpected_percent": round(pct, 2),
        })

    total_rules = len(rules)
    score = passed / total_rules * 100
    print(f"\n  Resultado: {passed}/{total_rules} reglas pasadas - {score:.1f}% de calidad")

    return {
        "domain":       name,
        "file":         filepath,
        "rows":         len(df),
        "rules_passed": passed,
        "rules_total":  total_rules,
        "quality_score": round(score, 2),
        "details":      results,
    }


def main():
    print("\n SAP Data Quality — Great Expectations")
    print(" Dominios: KNA1 (Clientes) · LFA1 (Proveedores)\n")

    all_results = []
    for domain_name, filename in DATA_FILES.items():
        rules = RULES[domain_name]
        result = validate_domain(domain_name, filename, rules)
        all_results.append(result)

    # Reporte final
    print(f"\n{'='*60}")
    print("  RESUMEN GENERAL")
    print(f"{'='*60}")
    total_score = 0
    for r in all_results:
        if r.get("skipped"):
            continue
        score = r["quality_score"]
        total_score += score
        bar = "#" * int(score / 5) + "-" * (20 - int(score / 5))
        print(f"  {r['domain']:<22} [{bar}] {score:.1f}%")

    avg = total_score / len([r for r in all_results if not r.get("skipped")])
    print(f"\n  Score promedio: {avg:.1f}%")

    # Exportar JSON
    output_path = Path("validation_results.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(all_results, f, ensure_ascii=False, indent=2)
    print(f"\n  Reporte exportado: {output_path}")


if __name__ == "__main__":
    main()
