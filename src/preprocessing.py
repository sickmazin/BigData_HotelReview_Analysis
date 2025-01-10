from functools import reduce
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DoubleType
from pyspark.sql.functions import regexp_replace, to_date, col, when, expr, split, regexp_replace


spark = SparkSession.builder \
    .appName("Hotel Review Statistics") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.local.dir", "/tmp/spark-temp") \
    .getOrCreate()

schema = StructType([
    StructField("Hotel_Address", StringType(), False),
    StructField("Additional_Number_of_Scoring", IntegerType(), False), 
    StructField("Review_Date", StringType(), False),
    StructField("Average_Score", FloatType(), False),  
    StructField("Hotel_Name", StringType(), False),
    StructField("Reviewer_Nationality", StringType(), False),
    StructField("Negative_Review", StringType(), False),
    StructField("Review_Total_Negative_Word_Counts", IntegerType(), False),
    StructField("Total_Number_of_Reviews", IntegerType(), False),
    StructField("Positive_Review", StringType(), False),
    StructField("Review_Total_Positive_Word_Counts", IntegerType(), False),
    StructField("Total_Number_of_Reviews_Reviewer_Has_Given", IntegerType(), False),
    StructField("Reviewer_Score", FloatType(), False),
    StructField("Tags", StringType(), False),
    StructField("days_since_review", StringType(), False),
    StructField("lat", DoubleType(), False),
    StructField("lng", DoubleType(), False)
])

reviews = spark.read.csv("data/Hotel_Reviews.csv", schema=schema, header=True)

#days_since_review column has string values, so we need to convert it to integer
reviews = reviews.withColumn("days_since_review", regexp_replace("days_since_review", " days| day", "").cast(IntegerType()))
#Review_Date column has string values and bad format, so we need to convert it to the right one
reviews = reviews.withColumn("Review_Date", to_date("Review_Date", "M/d/yyyy"))

#fixing empty postive and negative reviews rows by erasing respectively "No Negative" and "No Positive" values
reviews = reviews.withColumn("Negative_Review", when(col("Negative_Review") == "No Negative", "").otherwise(col("Negative_Review")))
reviews = reviews.withColumn("Positive_Review", when(col("Positive_Review") == "No Positive", "").otherwise(col("Positive_Review")))

#Tags colum converted in array 
reviews = reviews.withColumn("Tags_Cleaned", regexp_replace(col("Tags"), r"[\[\]']", ""))
reviews = reviews.withColumn("Tags_Array", split(col("Tags_Cleaned"), ",\s*")).drop("Tags_Cleaned", "Tags")
reviews = reviews.withColumn("Tags_Array", expr("transform(Tags_Array, x -> trim(x))"))

cols = [col(c) for c in reviews.columns]
filter_expr = reduce(lambda a, b: a | b.isNull(), cols[1:], cols[0].isNull())

all_null_values = reviews.filter(filter_expr)
print("all null valuses count: "+str(all_null_values.count()))


my_list = [("Mercure Paris Gare Montparnasse", 48.839752, 2.323791),
        ("Holiday Inn Paris Montmartre", 48.889212, 2.333239),
        ("Maison Albar Hotel Paris Op ra Diamond", 48.8753896, 2.3233823),
        ("NH Collection Barcelona Podium", 41.3918780, 2.1779369),
        ("City Hotel Deutschmeister", 48.2210163, 16.3666115),
        ("Cordial Theaterhotel Wien", 48.2097133, 16.3514418),
        ("Fleming s Selection Hotel Wien City", 48.2095233, 16.3535636),
        ("Hotel Park Villa", 48.2336527, 16.3458807),
        ("Hotel Daniel Vienna", 48.1889864, 16.3837793),
        ("Renaissance Barcelona Hotel", 41.3928700, 2.1673869),
        ("Roomz Vienna", 48.1868055, 16.4205255),
        ("Austria Trend Hotel Schloss Wilhelminenberg Wien", 48.2197338, 16.2855277),
        ("Hotel Advance", 41.3834277, 2.1629281),
        ("Derag Livinghotel Kaiser Franz Joseph Vienna", 48.2461265, 16.3412116),
        ("Hotel City Central", 48.2137371, 16.3799296),
        ("Hotel Atlanta", 48.2205819, 16.3557971),
        ("Hotel Pension Baron am Schottentor", 48.2168996, 16.3599271)]

for hotel in my_list:
    reviews = reviews.withColumn("lat", when(col("Hotel_Name") == hotel[0], hotel[1]).otherwise(col("lat"))) \
                     .withColumn("lng", when(col("Hotel_Name") == hotel[0], hotel[2]).otherwise(col("lng")))
   

reviews.write.save("data.parquet", format="parquet")