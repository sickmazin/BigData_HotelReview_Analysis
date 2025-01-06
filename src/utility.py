import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


'''
    These functions assume a certain type of DataFrame, which is the one obtained from the data folder.
'''


def get_spark_session():
    return SparkSession.builder \
                        .master("local[*]") \
                        .appName("Hotel Review Statistics") \
                        .config("spark.driver.memory", "8g") \
                        .config("spark.executor.memory", "8g") \
                        .config("spark.local.dir", "/tmp/spark-temp") \
                        .config("spark.executor.instances", "1") \
                        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
                        .config("spark.sql.execution.arrow.maxRecordsPerBatch", "1000") \
                        .getOrCreate()


def controversial_reviews(spark_session: SparkSession,  name: str, positive: bool = None, param :float = 3.4, hotel: str = None) -> DataFrame:
    '''
        This function returns a DataFrame where the reviews differ by a lot (parameter = 3.4) with respect to the average hotel score.
    '''

    df = spark_session.sql(f'''SELECT Positive_Review,
                            Hotel_Name, 
                            Negative_Review, 
                            (Average_Score-Reviewer_Score-3.4)+
                            (Review_Total_Negative_Word_Counts+
                            Review_Total_Positive_Word_Counts)/50 as Score FROM
                            {name} WHERE abs(Average_Score-Reviewer_Score) > {param}''')
    if hotel is not None:
        df = df.filter(df.Hotel_Name == hotel)
    if positive is not None:
        if filter:
            df = df.filter(df.Score > 0)
        else:
            df = df.filter(df.Score < 0)
    
    return df.withColumn("Score", F.abs(df.Score)).orderBy("Score", ascending=False).drop("Hotel_Name")\
             .withColumn("Review", F.concat(F.col("Positive_Review"), F.lit(" "), F.col("Negative_Review")))\
             .drop("Positive_Review", "Negative_Review")

                

def avg_points_per_period(spark_session: SparkSession, name: str, period: str = "month", hotel: str = None) -> DataFrame:
    '''
        This function returns a DataFrame with the average reviewer score for the specified period. Optional filtering with the hotel name is possible
    '''

    df = spark_session.sql(f"SELECT Review_Date, Hotel_Name, Reviewer_Score FROM {name}")
    
    if hotel is not None:
        df = df.filter(df.Hotel_Name == hotel)

    if period == "Day":
        return df.groupBy("Review_Date").agg(F.avg("Reviewer_Score").alias("Average_Score")) \
                 .orderBy("Review_Date").withColumnRenamed("Review_Date", "Day")
    elif period == "Month":
        return df.withColumn("Month", F.date_format(F.col("Review_Date"), "yyyy-MM")) \
                 .groupBy("Month").agg(F.avg("Reviewer_Score").alias("Average_Score")) \
                 .orderBy("Month")
    elif period == "Year":
        return df.withColumn("Year", F.date_format(F.col("Review_Date"), "yyyy")) \
                 .groupBy("Year").agg(F.avg("Reviewer_Score").alias("Average_Score")) \
                 .orderBy("Year")
    else:
        raise ValueError("Invalid period parameter")
    

def number_reviews_per_score(spark_session: SparkSession, name: str, hotel: str = None) -> DataFrame:
    df = spark_session.sql(f"SELECT Reviewer_Score, Hotel_Name FROM {name}").withColumn("Reviewer_Score", F.round(F.col("Reviewer_Score"), 0))
    if hotel is not None:
        df = df.filter(df.Hotel_Name == hotel)
    return df.groupBy("Reviewer_Score").count().orderBy("Reviewer_Score")
