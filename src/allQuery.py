import os
from functools import reduce

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date
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

spark_session = get_spark_session()

project_directory = os.path.dirname(os.path.abspath(__file__))
df = spark_session.read.parquet(os.path.join(project_directory, "data.parquet"))

#ESEGUIRE UNA PRIMA FASE DI CORREZIONE DELLO SCHEMA
reviews = df.withColumn("Review_Date", to_date(F.col("Review_Date"), "M/d/yyyy"))
reviews.createOrReplaceTempView("Hotel_Reviews")

#spark_session = get_spark_session()
#project_directory = os.path.dirname(os.path.abspath(__file__))
#
#reviews = spark_session.read.parquet(os.path.join(project_directory, "data.parquet"))
#reviews.createOrReplaceTempView("Hotel_Reviews")


def controversial_reviews(positive: bool = None, param :float = 3.4, hotel: str = None) -> DataFrame:
    '''
        This function returns a DataFrame where the reviews differ by a lot (parameter = 3.4) with respect to the average hotel score.
    '''

    df = spark_session.sql(f'''SELECT Positive_Review,
                            Hotel_Name, 
                            Negative_Review, 
                            (Average_Score-Reviewer_Score-3.4)+
                            (Review_Total_Negative_Word_Counts+
                            Review_Total_Positive_Word_Counts)/50 as Score FROM
                            Hotel_Reviews WHERE abs(Average_Score-Reviewer_Score) > {param}''')
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

def avg_points_per_period(period: str = "month", hotel: str = None) -> DataFrame:
    '''
        This function returns a DataFrame with the average reviewer score for the specified period. Optional filtering with the hotel name is possible
    '''

    df = spark_session.sql(f"SELECT Review_Date, Hotel_Name, Reviewer_Score FROM Hotel_Reviews")
    
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

def number_reviews_per_score( hotel: str = None) -> DataFrame:
    df = spark_session.sql(f"SELECT Reviewer_Score, Hotel_Name FROM Hotel_Reviews").withColumn("Reviewer_Score", F.round(F.col("Reviewer_Score"), 0))
    if hotel is not None:
        df = df.filter(df.Hotel_Name == hotel)
    return df.groupBy("Reviewer_Score").count().orderBy("Reviewer_Score")

def base_filtering(filter_Name: str, filter_Rev_Score: (float,str), filter_Avg_Score : (float,str), filter_Nationality_Rev, filter_Negative_Rev:bool, filter_Positive_Rev:bool, filter_Date,word_to_search):
    """
        Funzione per il filtraggio base su tutte le recensioni degli Hotel a seconda di diverse caratteristiche:
    :param filter_Name: nome perfetto dell'hotel
    :param filter_Rev_Score: valore della colonna Reviewer_Score e > o <, avere una tupla del tipo (6,<) o (7.2,>).
    :param filter_Avg_Score: valore della colonna Avg_Score e > o <, avere una tupla del tipo (6,<) o (7.2,>).
    :param filter_Nationality_Rev: valore della nazionalità del recensore, attento ai whitespace...
    :param filter_Negative_Rev: booleano se vuole visualizzare le recensioni negative, default si -> True
    :param filter_Positive_Rev: booleano se vuole visualizzare le recensioni positive, default si -> True
    :param filter_Date: data che indica che vuole vedere le recensioni fino al quel giorno.
    :return: Un DataFrame PySpark con le recensioni filtrate.
    """
    dfCopia=reviews
    #FILTRAGGIO PER NOME
    if filter_Name is not None:
        dfCopia=dfCopia.filter(dfCopia.Hotel_Name == filter_Name)

    #FILTRAGGIO PER VOTO RECENSIONE
    if filter_Rev_Score is not None:
        if filter_Rev_Score[1]==">":
            dfCopia= dfCopia.filter(dfCopia.Reviewer_Score >= filter_Rev_Score[0])
        elif filter_Rev_Score[1]=="<":
            dfCopia= dfCopia.filter(dfCopia.Reviewer_Score <= filter_Rev_Score[0])
        #else sarebbe errore da verificare

    #FILTRAGGIO PER VOTO MEDIO HOTEL
    if filter_Avg_Score is not None:
        if filter_Avg_Score[1]==">":
            dfCopia= dfCopia.filter(dfCopia.Reviewer_Score >= filter_Avg_Score[0])
        elif filter_Avg_Score[1]=="<":
            dfCopia= dfCopia.filter(dfCopia.Reviewer_Score <= filter_Avg_Score[0])
        #else sarebbe errore da verificare

    #FILTRAGGIO NAZIONALITà REVIEWER
    if filter_Nationality_Rev is not None:
        dfCopia= dfCopia.filter(dfCopia.Reviewer_Nationality == filter_Nationality_Rev)

    #FILTRAGGIO SE DEVE O MENO CONTENERE LA PARTE NEGATIVA
    if filter_Negative_Rev is False:
        dfCopia = dfCopia.drop(dfCopia.Negative_Review)

    #FILTRAGGIO SE DEVE O MENO CONTENERE LA PARTE POSITIVA
    if filter_Positive_Rev is False:
        dfCopia = dfCopia.drop(dfCopia.Positive_Review)

    #FILTRAGGIO PER DATA
    if filter_Date is not None:
        dfCopia = dfCopia.filter(dfCopia.Review_Date <= filter_Date)

    if word_to_search is not None:
        dfCopia= filter_reviews_by_word(dfCopia, word_to_search)

    return dfCopia

def rev_pos_ge_neg():
    """
    Filtra le recensioni che contengono un numero di parole positive maggiore o uguale rispetto alle parole negative
    e un punteggio del recensore maggiore o uguale a 6.

    :return: Un DataFrame contenente le recensioni filtrate, in cui:
             - `Review_Total_Positive_Word_Counts >= Review_Total_Negative_Word_Counts`
             - `Reviewer_Score >= 6`
    """
    return spark_session.sql("Select * from Hotel_Reviews where Review_Total_Positive_Word_Counts >= Review_Total_Negative_Word_Counts AND Reviewer_Score >= 6")

def top_nationality_rev(modality: int):
    """
    Restituisce la classifica delle nazionalità dei recensori in base a diversi criteri.

    :param modality: Un intero che specifica il criterio di aggregazione:
                     - 1: Classifica delle nazionalità in base alla media del punteggio dei recensori (`Reviewer_Score`).
                     - 2: Classifica delle nazionalità in base al numero di recensioni.
                     - 3: Classifica delle nazionalità in base al totale delle parole utilizzate nelle recensioni, sommando
                          le parole positive e negative.

    :return: Un DataFrame contenente la classifica delle nazionalità dei recensori in base al criterio specificato:
             - Modalità 1: Media dei punteggi dei recensori per ogni nazionalità.
             - Modalità 2: Numero totale di recensioni per ogni nazionalità.
             - Modalità 3: Numero totale di parole (somma di parole positive e negative) per ogni nazionalità.
    """
    dfCopia = reviews
    if modality == 1:
        dfCopia = (dfCopia.groupby(dfCopia.Reviewer_Nationality)
                   .avg("Reviewer_Score")
                   .sort("avg(Reviewer_Score)", ascending=False)) \
            .select("Reviewer_Nationality", "avg(Reviewer_Score)")
    elif modality == 2:
        dfCopia = (dfCopia.groupby(dfCopia.Reviewer_Nationality).count()
                   .withColumnRenamed("count", "Totale recensioni")
                   .sort("Totale recensioni", ascending=False)) \
            .select("Reviewer_Nationality", "Totale recensioni")
    elif modality == 3:
        dfCopia = dfCopia.groupby("Reviewer_Nationality") \
            .agg(
            F.sum("Review_Total_Positive_Word_Counts").alias("Total_Positive"),
            F.sum("Review_Total_Negative_Word_Counts").alias("Total_Negative"),
            F.sum("Reviewer_Score").alias("Num Review"),
        ) \
            .withColumn("Totale numero parole", F.col("Total_Positive") + F.col("Total_Negative")/ F.col("Num Review")) \
            .sort("Totale numero parole", ascending=False) \
            .select("Reviewer_Nationality", "Totale numero parole")
    return dfCopia

def top_hotel_rev(modality: int):
    """
    :param modality:
        1 for top about hotel for mean of avg_score,
        2 for top about hotel with most number of review,
        3 for top about hotel with most review words counts.
    """
    dfCopia=reviews
    if modality == 1:
        dfCopia= dfCopia.groupby(dfCopia.Hotel_Name).avg("Average_Score").sort("avg(Average_Score)", ascending=False) \
            .withColumnRenamed("avg(Average_Score)", "Totale recensioni") \
            .select("Hotel_Name", "Totale recensioni")

    elif modality == 2:
        dfCopia= dfCopia.groupby(dfCopia.Hotel_Name).count().withColumnRenamed("count","Totale recensioni") \
            .sort("Totale recensioni", ascending=False) \
            .select("Hotel_Name", "Totale recensioni")

    elif modality == 3:
        dfCopia = (dfCopia.groupby(dfCopia.Hotel_Name) \
                   .agg(
            F.sum("Review_Total_Positive_Word_Counts").alias("Total_Positive"),
            F.sum("Review_Total_Negative_Word_Counts").alias("Total_Negative")
        ) \
                   .withColumn("Totale numero parole", F.col("Total_Positive") + F.col("Total_Negative")) \
                   .sort("Totale numero parole", ascending=False) \
                   .select("Hotel_Name", "Totale numero parole"))


    return dfCopia

def plot_reviews_by_dimension(Hotel_Name: str, Nationality: str):
    """
        Plotting number of reviews (y) by data (x) filtered by hotel, nazionality
    """
    dfCount= reviews
    if Hotel_Name is not None:
        dfCount = dfCount.filter(dfCount.Hotel_Name == Hotel_Name)
    elif Nationality is not None:
        dfCount = dfCount.filter(dfCount.Reviewer_Nationality == Nationality)
    dfCount=dfCount.groupby(dfCount.Review_Date).count().withColumnRenamed("count","Rev_For_Date")
    return dfCount.select("Rev_For_Date","Review_Date")

def all_reviews_score_by_Hotel(Hotel_Name: str):
    dfRet= reviews
    if Hotel_Name is not None:
        dfRet = dfRet.filter(dfRet.Hotel_Name == Hotel_Name)
    return dfRet.select("Hotel_Name", "Reviewer_Score")

def best_tags():
    """
    :return:  Un DataFrame PySpark con i tags ordinati per numero di utilizzi Tag-Count.
    """
    #explode separa ogni elemento dell'array in una nuova riga
    df_exploded = reviews.withColumn("Tag", F.explode(F.col("Tags_Array")))

    tag_count = df_exploded.groupBy("Tag").count().orderBy("count", ascending=False)

    return tag_count

def filter_reviews_by_word(dfNew, word: str):
    """
    Filtra le recensioni che contengono una determinata parola.

    :param word: La parola da cercare nelle recensioni.
    :return: Un DataFrame PySpark con le recensioni filtrate.
    """
    # Filtra le recensioni positive o negative che contengono la parola
    filtered_df = dfNew.filter(
        (F.col("Positive_Review").contains(word)) |
        (F.col("Negative_Review").contains(word))
    )
    return filtered_df

def top_rev_score(modality: int):
    dfCopia = reviews
    if modality==1:
        dfCopia= (dfCopia.groupby(dfCopia.Reviewer_Score).count()
                  .sort("count", ascending=False)
                  .withColumnRenamed("count", "Totale recensioni"))
    elif modality==2:
        dfCopia = dfCopia.withColumn(
            'Review_Type',
            F.when(dfCopia['Reviewer_Score'] >= 8, 'Positive')
            .when(dfCopia['Reviewer_Score'] <= 4.5, 'Negative')
            .otherwise('Neutral')
        )
        dfCopia= dfCopia.groupBy('Review_Type').agg(
            F.count('*').alias('Totale recensioni')
        )
    return dfCopia

def review_plot_yearORmonths(modality: int, hotel: str):
    """
    Restituisce il numero di recensioni per anno, mese o per mese in un anno specifico, a seconda della modalità scelta.
    I risultati possono essere filtrati per un hotel specifico, se fornito.

    :param modality: Un intero che specifica il tipo di aggregazione:
                     - 1: Numero di recensioni per mese, per tutti gli anni.
                     - 2: Numero di recensioni per anno.
                     - 3: Numero di recensioni per mese in un anno specifico.
    :param hotel: Il nome dell'hotel per cui si desidera visualizzare le recensioni. Se `None`, vengono considerati tutti gli hotel nel dataset.

    :return: Un DataFrame contenente il numero di recensioni aggregato per anno e/o mese, in base alla modalità scelta.
            La struttura del DataFrame dipenderà dalla modalità:
            - Modalità 1: DataFrame con colonne `Month` e `Review_Count`.
            - Modalità 2: DataFrame con colonne `Year` e `Review_Count`.
            - Modalità 3: DataFrame con colonne `Year`, `Month` e `Review_Count`.

    Esempio di utilizzo:
    ```
    # Per ottenere il numero di recensioni per mese per tutti gli anni
    review_plot_yearORmonths(1, None)

    # Per ottenere il numero di recensioni per anno per un hotel specifico
    review_plot_yearORmonths(2, "Hotel Name")

    # Per ottenere il numero di recensioni per mese in un anno specifico per un hotel
    review_plot_yearORmonths(3, "Hotel Name")
    ```
    """
    if hotel is not None:
        dfUtils = reviews.filter(reviews.Hotel_Name == hotel)
    else:
        dfUtils = reviews

    res = reviews

    if modality == 1:
        res = dfUtils \
            .groupBy(F.month("Review_Date").alias("Month")) \
            .agg(F.count("*").alias("Review_Count")) \
            .orderBy("Month")

    if modality == 2:
        res = dfUtils \
            .groupBy(F.year("Review_Date").alias("Year")) \
            .agg(F.count("*").alias("Review_Count")) \
            .orderBy("Year")

    if modality == 3:
        res = dfUtils \
            .groupBy(F.year("Review_Date").alias("Year"), F.month("Review_Date").alias("Month")) \
            .agg(F.count("*").alias("Review_Count")) \
            .orderBy("Year", "Month")

    return res

def dataMap():
    return spark_session.sql("SELECT Hotel_Name, count(*) as Reviews, first(Average_Score) as Average_Score, first(lat) as lat, first(lng) as lng FROM Hotel_Reviews GROUP BY Hotel_Name").toPandas()

def dataFrameForModel():
    dfRet=reviews.withColumn("Review", F.concat(F.col("Positive_Review"), F.lit(" "), F.col("Negative_Review")))
    return  dfRet.withColumn("Reviewer_Score", F.when(dfRet.Reviewer_Score >= 7, 1).otherwise(0)).withColumnRenamed("Reviewer_Score", "label")

# FUNZIONI UTILI PER FILTERING

def all_nationality():
    return  [row["Reviewer_Nationality"] for row in reviews.select("Reviewer_Nationality").distinct().collect()]

def get_Rev_Score():
    dfRet=reviews.withColumn("Review", F.concat(F.col("Positive_Review"), F.lit(" "), F.col("Negative_Review")))
    return dfRet.select("Review", "Reviewer_Score")

def all_Hotel_Name():
    return [row["Hotel_Name"] for row in reviews.select("Hotel_Name").distinct().collect()]

def number_of_Hotel():
    return reviews.select("Hotel_Name").distinct().count()

def topDict(df,head):
    hotel_Name_list = []
    rev_number_list = []
    for row in df.head(head):
        hotel_Name_list.append(row[0])
        rev_number_list.append(row[1])
    dict= {df.columns[0]: hotel_Name_list, df.columns[1]: rev_number_list}
    return dict

