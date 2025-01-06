import utility as u
import os
import streamlit as st
import pandas as pd
import folium
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame, Column
from streamlit_folium import st_folium
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, StopWordsRemover
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from wordcloud import WordCloud
import matplotlib.pyplot as plt


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

@st.cache_data
def get_data():
    return spark.sql("SELECT Hotel_Name, count(*) as Reviews, first(Average_Score) as Average_Score, first(lat) as lat, first(lng) as lng FROM Hotel_Reviews GROUP BY Hotel_Name").toPandas()

@st.cache_data
def popup(name, reviews, score):
    return  f'''
    <div style="width: 200px; height: 120px; padding: 5px; border-radius: 8px; text-align:center;">
            <strong>{name}</strong>
            <div style="display:block;padding:5px;align-items:center;justify-content:space-between">
                <div style="text-align:center;">
                <strong>Punteggio: {score}/10 </strong>
                </div>
                <div style="text-align:center;">
                <strong>Recensioni: {reviews}</strong>
                </div>
            </div>
        </div>'''

@st.cache_data
def get_wordclouds(_wc_review: DataFrame):
    positive = _wc_review.filter(_wc_review.Reviewer_Score >= 7).toPandas()
    text = " ".join(str(value) for value in positive["Review"])
    p_wordcloud = WordCloud(
            width= 300,
            height= 200,
            background_color = 'white',
            max_words = 200,
            max_font_size = 40, 
            scale = 3,
            random_state = 42
        ).generate(text)

    negative = _wc_review.filter(_wc_review.Reviewer_Score < 7).toPandas()
    text = " ".join(str(value) for value in negative["Review"])
    n_wordcloud = WordCloud(
            width= 300,
            height= 200,
            background_color = 'white',
            max_words = 200,
            max_font_size = 40, 
            scale = 3,
            random_state = 42
        ).generate(text)

    return p_wordcloud, n_wordcloud

spark = u.get_spark_session()

project_directory = os.path.dirname(os.path.abspath(__file__))

reviews = spark.read.parquet(os.path.join(project_directory, "data.parquet"))
reviews.createOrReplaceTempView("Hotel_Reviews")

df = reviews.withColumn("Review", F.concat(F.col("Positive_Review"), F.lit(" "), F.col("Negative_Review")))

review_score = df.select("Review", "Reviewer_Score")
model = create_sentment_model(review_score)


st.title("Hotel Review Statistics")


center_eu = [54.5260, 15.2551] 
map = folium.Map(location=center_eu, zoom_start=5, prefer_canvas=True, tiles="cartodb positron")
marker_cluster = folium.plugins.MarkerCluster().add_to(map)

df = get_data()

 
for row in df.itertuples():
    folium.Marker([row.lat, row.lng], popup=folium.Popup(popup(row.Hotel_Name, row.Reviews, round(row.Average_Score, 1)), max_width=250)).add_to(marker_cluster)

st_folium(map, width=700, height=500)

p_wordcloud, n_wordcloud = get_wordclouds(review_score)

fig, axes = plt.subplots(1, 2, figsize=(12, 6))

axes[0].imshow(p_wordcloud, interpolation='bilinear')
axes[0].set_title("Word Cloud of positive reviews", fontsize=14)
axes[0].axis('off')

axes[1].imshow(n_wordcloud, interpolation='bilinear')
axes[1].set_title("Word Cloud of negative reviews", fontsize=14)
axes[1].axis('off')

st.pyplot(fig)

hotel_names = [None] + df.Hotel_Name.tolist()
hotel1 = st.selectbox("Select hotel", hotel_names, format_func=lambda x: "None" if x is None else x, key="hotel1")

reviews_by_score = u.number_reviews_per_score(spark, "Hotel_Reviews", hotel1).toPandas()
st.bar_chart(reviews_by_score, x="Reviewer_Score", y="count", x_label="Reviewer score", use_container_width=True)

col1, col2 = st.columns(2)

hotel2 = col1.selectbox("Select hotel", hotel_names, format_func=lambda x: "None" if x is None else x, key="hotel2")
period = col2.selectbox("Select period", ["Day", "Month", "Year"])

st.line_chart(u.avg_points_per_period(spark, "Hotel_Reviews", period, hotel2).toPandas(), x = period, x_label=period, y="Average_Score", use_container_width=True)

col1, col2, col3, col4 = st.columns(4)
hotel3 = col1.selectbox("Select hotel", hotel_names, format_func=lambda x: "None" if x is None else x, key="hotel3")
param = col2.number_input("Insert parameter", value=3.4, step=0.1)
positive = col3.selectbox("Positive", [None, "Positive", "Negative"], format_func=lambda x: "None" if x is None else x)
ascending_cb = col4.checkbox("Ascending", value=False)

controversial_reviews = u.controversial_reviews(spark, "Hotel_Reviews", positive=positive, param=param, hotel=hotel3).toPandas()

controversial_reviews.sort_values(by="Score", ascending=ascending_cb)
st.write(controversial_reviews)


input = st.chat_input("Insert a review to be checked")
model = create_sentment_model()
if input is not None:
    output = model.transform(spark.createDataFrame(pd.DataFrame({"Review": [input]})))
    prediction = int(output.select('prediction').collect()[0][0])
    probs = output.select('probability').collect()[0][0]
    result = "Positive" if prediction==1 else "Negative"
    st.write(f"The model assessed the review as {result} with probability {probs[prediction]:.2f}")