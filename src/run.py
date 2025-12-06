from pyspark import pipelines as dp
from ingestion.load_raw import load_raw
from transformations.cleaning_products import transform_products
from transformations.explode_ingredients import explode_ingredients
from pyspark.sql import functions as F


# Please edit the sample below


@dp.table
def run():
    raw = load_raw(spark, "/Volumes/workspace/bb_data/other_cf/*")
    
    df = transform_products(raw)
    df_ing = explode_ingredients(df)

    df_prod = df.select("product_id", "name", "brand", "category", "price", "size", "rate", "description", "usage")
    df_comp = df_ing.select("ingredient_id", "ingredients_clean")\
                        .join(df.select("product_id", F.col("ingredients_clean").alias("ing2")), on=F.col("ingredients_clean")==F.col("ing2"), how="left")\
                        .select("ingredient_id", "product_id")\
                        .distinct()

    df_prod.write.mode("overwrite").saveAsTable("workspace.bb_data.products")
    df_ing.select("ingredient_id", "ing_name").distinct().write.mode("overwrite").saveAsTable("workspace.bb_data.ingredients")
    df_comp.write.mode("overwrite").saveAsTable("workspace.bb_data.composition")
    
    return df