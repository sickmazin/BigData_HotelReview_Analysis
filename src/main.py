from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Hotel Review Statistics") \
    .master("local[*]") \
    .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
    .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow") \
    .getOrCreate()

print(spark.version)