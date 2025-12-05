# Pipeline PySpark pour la normalisation des produits cosmétiques – Databricks

## Description

- ingestion CSV
- nettoyage
- normalisation
- extraction ingrédients
- construction des 3 tables
- UUID
- Delta Lake

# Techniques utilisées

Databricks
PySpark
Delta Lake
Spark SQL
Régex Python
Pipelines de transformation

# Architecture

Ajoute un petit schéma simple (flux → transformation → tables Delta)

# Objectif du pipeline

Créer un modèle produit / ingrédient exploitable par PowerBI / Looker.

```text
databricks_pipeline/
├── README.md
├── src/
│   ├── ingestion/
│   │   └── load_raw.py
│   ├── transformations/
│   │   ├── clean_products.py
│   │   └── explode_ingredients.py
│   ├── utils/
│   │   └── cleaning_utils.py
│   └── run.py
```