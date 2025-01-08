import streamlit as st
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, StopWordsRemover
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
import pandas as pd
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from allQuery import spark_session


@st.cache_resource
def create_sentment_model(_dataframe: DataFrame) -> Pipeline:
    dataframe = _dataframe.withColumn("Reviewer_Score", F.when(_dataframe.Reviewer_Score >= 7, 1).otherwise(0)).withColumnRenamed("Reviewer_Score", "label")

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
    return pipeline_model

input = st.chat_input("Insert a review to be checked")
model = create_sentment_model()
if input is not None:
    output = model.transform(spark_session.createDataFrame(pd.DataFrame({"Review": [input]})))
    prediction = int(output.select('prediction').collect()[0][0])
    probs = output.select('probability').collect()[0][0]
    result = "Positive" if prediction==1 else "Negative"
    st.write(f"The model assessed the review as {result} with probability {probs[prediction]:.2f}")
