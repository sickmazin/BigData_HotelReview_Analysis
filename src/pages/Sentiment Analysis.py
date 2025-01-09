import streamlit as st
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, StopWordsRemover
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
import pandas as pd
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from allQuery import spark_session, dataFrameForModel


@st.cache_resource
def create_sentment_model() -> Pipeline:
    dataframe = dataFrameForModel()

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

st.title("Modello di Sentiment Analysis")

st.markdown("""
Il modello di sentiment analysis sviluppato offre una soluzione semplice ed efficace per analizzare le recensioni.  
Inserendo una recensione, il modello è in grado di identificare automaticamente la tipologia del sentimento espresso, classificandolo come **positivo** o **negativo**.  

Questo strumento, basato su algoritmi avanzati di elaborazione del linguaggio naturale, consente di ottenere:  
- **Risultati rapidi e accurati**  
- **Un processo di analisi semplificato**  

Prova subito il nostro modello inserendo una tua possibile recensione, ricorda : che sia in inglese!
""")

input = st.chat_input("Insert a review to be checked")
model = create_sentment_model()
if input is not None:
    output = model.transform(spark_session.createDataFrame(pd.DataFrame({"Review": [input]})))
    prediction = int(output.select('prediction').collect()[0][0])
    probs = output.select('probability').collect()[0][0]
    result = "Positive" if prediction==1 else "Negative"
    st.write(f"The model assessed the review as {result} with probability {probs[prediction]:.2f}")
