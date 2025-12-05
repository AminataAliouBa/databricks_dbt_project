import uuid
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from src.utils.cleaning_utils import clean_null_and_esc, normalize_ingredients

def transform_products(df):
    df = clean_null_and_esc(df)
    df = normalize_ingredients(df)

    df = df.withColumn("rate", F.col("rate").try_cast("int"))\
            .select([F.upper(F.col(c)).alias(c) for c in df.columns])\
            .filter(F.col("name").isNotNull())\
            .filter(F.col("name") != "NULL")\
            .withColumn( "brand",F.when(F.length(F.trim(F.col("brand"))) == 0, "UNKNOWN").otherwise(F.col("brand")))\
            .distinct()\
            .withColumn("product_id", F.expr("uuid()"))
    return df
