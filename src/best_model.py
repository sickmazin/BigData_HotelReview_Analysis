import allQuery as u
import os
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, StopWordsRemover
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql import functions as F

spark = u.get_spark_session()

src_directory = os.path.dirname(os.path.abspath(__file__))
reviews = spark.read.parquet(os.path.join(src_directory,"data.parquet"))

dataframe = reviews.select("Positive_Review", "Negative_Review", "Reviewer_Score")
dataframe = dataframe.withColumn("Review", F.concat(dataframe.Positive_Review, F.lit(" "), dataframe.Negative_Review)) \
                    .drop("Positive_Review", "Negative_Review")

dataframe = dataframe.withColumn("Reviewer_Score", F.when(dataframe.Reviewer_Score >= 7, 1).otherwise(0)).withColumnRenamed("Reviewer_Score", "label")



tokenizer = Tokenizer(inputCol="Review", outputCol="words")
remover = StopWordsRemover(inputCol="words", outputCol="filtered")
hashingTF = HashingTF(inputCol="filtered", outputCol="rawFeatures", numFeatures=1000)
idf = IDF(inputCol="rawFeatures", outputCol="features")
log_reg = LogisticRegression(featuresCol="features", labelCol="label", maxIter=20, regParam=0.01, elasticNetParam=0.0)
eval = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")

# Separate classes and undersample positive class (the majority)
positive_df = dataframe.filter(dataframe.label == 1)
negative_df = dataframe.filter(dataframe.label == 0)
fraction = negative_df.count() / positive_df.count()
positive_sampled = positive_df.sample(withReplacement=False, fraction=fraction)

df = positive_sampled.union(negative_df)
train, test = df.randomSplit([0.8, 0.2], seed=42)

pipeline = Pipeline(stages=[tokenizer, remover, hashingTF, idf, log_reg])

pipeline_model = pipeline.fit(train)

roc_auc = eval.evaluate(pipeline_model.transform(test))
print(f"ROC-AUC for loaded model: {roc_auc:.3f}")