from pyspark.sql import functions as F

def clean_null_and_esc(df):
    df = df.select([
        F.when(F.col(c).isin("NULL",""), None).otherwise(F.col(c)).alias(c)
        for c in df.columns
    ])
    string_cols = [c for c, t in df.dtypes if t == "string"]
    for c in string_cols:
        df = df.withColumn(c, F.regexp_replace(F.col(c), r"[\r\t\n\\s+]", " "))
    return df

def normalize_ingredients(df):
    df = df.withColumn( "ingredients",F.when(F.length(F.trim(F.col("ingredients"))) == 0, "UNKNOWN").otherwise(F.col("ingredients")))\
                .filter(F.col("ingredients").isNotNull()) \
                .withColumn("ingredients_clean", F.regexp_replace(F.col("ingredients"), r"[\(;&,+]", ",")) \
                .withColumn("ingredients_clean", F.regexp_replace(F.col("ingredients_clean"), r"[\).]", "")) \
                .withColumn( "ingredients_clean",F.when(F.length(F.trim(F.col("ingredients_clean"))) == 0, "UNKNOWN").otherwise(F.col("ingredients_clean")))
    return df
