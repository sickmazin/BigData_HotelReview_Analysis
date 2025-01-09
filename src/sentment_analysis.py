from pyspark.ml.feature import HashingTF, IDF, Tokenizer, StopWordsRemover
from pyspark.ml.classification import LogisticRegression, LinearSVC
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml import Pipeline
from pyspark.sql import functions as F
import utility as u
import os

spark = u.get_spark_session()

project_directory = os.path.dirname(os.path.abspath(__file__))
reviews = spark.read.parquet(os.path.join(project_directory, "data.parquet"))

dataframe = reviews.select("Positive_Review", "Negative_Review", "Reviewer_Score")
dataframe = dataframe.withColumn("Review", F.concat(dataframe.Positive_Review, F.lit(" "), dataframe.Negative_Review)) \
                    .drop("Positive_Review", "Negative_Review")

dataframe = dataframe.withColumn("Reviewer_Score", F.when(dataframe.Reviewer_Score >= 7, 1).otherwise(0)).withColumnRenamed("Reviewer_Score", "label")

dataframe.groupBy("label").count().show()

# Separate classes and undersample positive class (the majority)
positive_df = dataframe.filter(dataframe.label == 1)
negative_df = dataframe.filter(dataframe.label == 0)


fraction = negative_df.count() / positive_df.count()
positive_sampled = positive_df.sample(withReplacement=False, fraction=fraction)

df = positive_sampled.union(negative_df)


tokenizer = Tokenizer(inputCol="Review", outputCol="words")
remover = StopWordsRemover(inputCol="words", outputCol="filtered")
hashingTF = HashingTF(inputCol="filtered", outputCol="rawFeatures", numFeatures=1000)
idf = IDF(inputCol="rawFeatures", outputCol="features")
log_reg = LogisticRegression(featuresCol="features", labelCol="label", maxIter=20)
eval = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")


svm = LinearSVC(featuresCol="features", labelCol="label")

pipeline_svm = Pipeline(stages=[tokenizer, remover, hashingTF, idf, svm])
pipeline_lr = Pipeline(stages=[tokenizer, remover, hashingTF, idf, log_reg])



grid_lr = ParamGridBuilder()\
        .addGrid(hashingTF.numFeatures, [10, 100, 1000])\
        .addGrid(log_reg.regParam, [0.1, 0.01])\
        .addGrid(log_reg.elasticNetParam, [0.0, 0.5])\
        .build()



grid_svm = ParamGridBuilder()\
        .addGrid(hashingTF.numFeatures, [10, 100, 1000])\
        .addGrid(svm.regParam, [1, 0.1, 0.01])\
        .build()

cross_val_svm = CrossValidator(estimator=pipeline_svm, 
                           estimatorParamMaps=grid_svm, 
                           evaluator=eval, 
                           numFolds=5)

cv_model_svm = cross_val_svm.fit(df)

bestModel = cv_model_svm.bestModel.stages[-1]
print("Best Parameters: ", bestModel.explainParams())

avgMetrics = cv_model_svm.avgMetrics  
print("Cross-Validation Metrics: ", avgMetrics)

cross_val_lr = CrossValidator(estimator=pipeline_svm, 
                           estimatorParamMaps=grid_svm, 
                           evaluator=eval, 
                           numFolds=5)

cv_model_lr = cross_val_lr.fit(df)

bestModel = cv_model_lr.bestModel.stages[-1]
print("Best Parameters: ", bestModel.explainParams())

avgMetrics = cv_model_lr.avgMetrics  
print("Cross-Validation Metrics: ", avgMetrics)

#cv_model.save(os.path.join(project_directory, "sentment_analysis"))

#best params for log_reg regparam=0.01, elasticnetparam=0.0, numfeatures=1000
#best params for svm regparam=0.01, numFeatures=1000