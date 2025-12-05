import uuid
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

def explode_ingredients(df):
    df_ing = df.select("product_id", "ingredients_clean") \
                .withColumn("ing_name", F.explode(F.split(F.col("ingredients_clean"), ","))) \
                .withColumn("ing_name", F.trim(F.col("ing_name"))) \
                .filter(F.col("ing_name").isNotNull()) \
                .filter(F.col("ing_name") != "NULL") \
                .filter(F.length(F.col("ing_name")) > 1) \
                .select("product_id", "ing_name", "ingredients_clean") \
                .distinct()\
                .dropDuplicates(["ing_name"])\
                .withColumn("ingredient_id", F.expr("uuid()"))
    return df_ing