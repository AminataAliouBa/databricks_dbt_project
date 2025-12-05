from pyspark.sql import functions as F

def load_raw(spark, path):
    return spark.read.csv(path, header=True, sep=';', inferSchema=True, quote='"', multiLine=True, escape='"')
