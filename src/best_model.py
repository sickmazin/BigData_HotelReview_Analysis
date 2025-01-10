import allQuery as u
import os
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, StopWordsRemover
from pyspark.ml.classification import LogisticRegression, LinearSVC
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql import functions as F
from sklearn.metrics import roc_curve
import matplotlib.pyplot as plt

spark = u.get_spark_session()

src_directory = os.path.dirname(os.path.abspath(__file__))
reviews = spark.read.parquet(os.path.join(src_directory,"data.parquet"))

dataframe = reviews.select("Positive_Review", "Negative_Review", "Reviewer_Score")
dataframe = dataframe.withColumn("Review", F.concat(dataframe.Positive_Review, F.lit(" "), dataframe.Negative_Review)) \
                    .drop("Positive_Review", "Negative_Review")

dataframe = dataframe.withColumn("Reviewer_Score", F.when(dataframe.Reviewer_Score >= 7, 1).otherwise(0)).withColumnRenamed("Reviewer_Score", "label")



tokenizer = Tokenizer(inputCol="Review", outputCol="words")
remover = StopWordsRemover(inputCol="words", outputCol="filtered")
hashingTF = HashingTF(inputCol="filtered", outputCol="rawFeatures", numFeatures=10000)
idf = IDF(inputCol="rawFeatures", outputCol="features")
log_reg = LogisticRegression(featuresCol="features", labelCol="label", maxIter=20, regParam=0.1, elasticNetParam=0.0)
svm = LinearSVC(featuresCol="features", labelCol="label", regParam=0.1)
eval = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")

# Separate classes and undersample positive class (the majority)
positive_df = dataframe.filter(dataframe.label == 1)
negative_df = dataframe.filter(dataframe.label == 0)
fraction = negative_df.count() / positive_df.count()
positive_sampled = positive_df.sample(withReplacement=False, fraction=fraction)

df = positive_sampled.union(negative_df)
train, test = df.randomSplit([0.8, 0.2], seed=42)

pipeline_lr = Pipeline(stages=[tokenizer, remover, hashingTF, idf, log_reg])

pipeline_model_lr = pipeline_lr.fit(train)

pipeline_svm = Pipeline(stages=[tokenizer, remover, hashingTF, idf, svm])

pipeline_model_svm = pipeline_svm.fit(train)

roc_auc_lr = eval.evaluate(pipeline_model_lr.transform(test))
print(f"ROC-AUC for log reg: {roc_auc_lr}")

transformed = pipeline_model_svm.transform(test)
predictions = transformed.select("label", "rawPrediction").toPandas()
roc_auc_svm = eval.evaluate(transformed)
print(f"ROC-AUC for svm: {roc_auc_svm}")

lr_model = pipeline_model_lr.stages[-1]

# Get the ROC data for log reg
roc = lr_model.summary.roc.toPandas()
# Get the ROC data for svm
fpr_svm, tpr_svm, thres = roc_curve(y_true=predictions["label"], y_score=predictions["rawPrediction"].apply(lambda x: x[1]))

# Plot the ROC curve
plt.figure(figsize=(8, 6))
plt.plot(roc['FPR'], roc['TPR'], label="ROC Curve Logistic Regression (AUC = {:.4f})".format(lr_model.summary.areaUnderROC))
plt.plot(fpr_svm, tpr_svm, label="ROC Curve SVM (AUC = {:.4f})".format(roc_auc_svm))
plt.plot([0, 1], [0, 1], 'k--', label="Random Classifier")
plt.xlabel("False Positive Rate")
plt.ylabel("True Positive Rate")
plt.title("ROC Curve")
plt.legend(loc="best")
plt.grid()
plt.show()