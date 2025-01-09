from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("Hotel Review Statistics") \
    .master("local[*]") \
    .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
    .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow") \
    .config("spark.driver.memory", "16g") \
    .config("spark.executor.memory", "16g") \
    .getOrCreate()
df = spark.read.csv("dataset/Hotel_Reviews.csv", header=True, inferSchema=True)

#ESEGUIRE UNA PRIMA FASE DI CORREZIONE DELLO SCHEMA
df = df.withColumn("Review_Date", to_date(F.col("Review_Date"), "M/d/yyyy"))


#QUERY



