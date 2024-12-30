from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("Hotel Review Statistics") \
    .master("local[*]") \
    .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
    .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow") \
    .getOrCreate()

df = spark.read.csv("../dataset/Hotel_Reviews.csv", header=True, inferSchema=True)

#ESEGUIRE UNA PRIMA FASE DI CORREZIONE DELLO SCHEMA
df = df.withColumn("Review_Date", to_date(F.col("Review_Date"), "M/d/yyyy"))

def base_filtering(filter_Name: str, filter_Rev_Score: (float,str), filter_Avg_Score : (float,str), filter_Nationality_Rev, filter_Negative_Rev:bool, filter_Positive_Rev:bool, filter_Date):
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
    dfCopia=df
    #FILTRAGGIO PER NOME
    if filter_Name is not None:
        print("Name")
        dfCopia=dfCopia.filter(df.Hotel_Name == filter_Name)

    #FILTRAGGIO PER VOTO RECENSIONE
    if filter_Rev_Score is not None:
        print("REv Score")
        if filter_Rev_Score[1]==">":
            dfCopia= dfCopia.filter(df.Reviewer_Score >= filter_Rev_Score[0])
        elif filter_Rev_Score[1]=="<":
            dfCopia= dfCopia.filter(df.Reviewer_Score <= filter_Rev_Score[0])
        #else sarebbe errore da verificare

    #FILTRAGGIO PER VOTO MEDIO HOTEL
    if filter_Avg_Score is not None:
        print("Avg Score")
        if filter_Avg_Score[1]==">":
            dfCopia= dfCopia.filter(df.Reviewer_Score >= filter_Avg_Score[0])
        elif filter_Avg_Score[1]=="<":
            dfCopia= dfCopia.filter(df.Reviewer_Score <= filter_Avg_Score[0])
        #else sarebbe errore da verificare

    #FILTRAGGIO NAZIONALITà REVIEWER
    if filter_Nationality_Rev is not None:
        print("Nationality")
        dfCopia= dfCopia.filter(df.Reviewer_Nationality == filter_Nationality_Rev)


    #FILTRAGGIO SE DEVE O MENO CONTENERE LA PARTE NEGATIVA
    if filter_Negative_Rev is False:
        print("Neg")
        dfCopia = dfCopia.drop(df.Negative_Review)

    #FILTRAGGIO SE DEVE O MENO CONTENERE LA PARTE POSITIVA
    if filter_Positive_Rev is False:
        print("Pos")
        dfCopia = dfCopia.drop(df.Positive_Review)

    #FILTRAGGIO PER DATA
    if filter_Date is not None:
        print("Date")
        dfCopia = dfCopia.filter(dfCopia.Review_Date <= to_date(filter_Date))

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
        dfUtils = df.filter(df.Hotel_Name == hotel)
    else:
        dfUtils = df

    res = df

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

def rev_pos_ge_neg():
    """
    Filtra le recensioni che contengono un numero di parole positive maggiore o uguale rispetto alle parole negative
    e un punteggio del recensore maggiore o uguale a 6.

    :return: Un DataFrame contenente le recensioni filtrate, in cui:
             - `Review_Total_Positive_Word_Counts >= Review_Total_Negative_Word_Counts`
             - `Reviewer_Score >= 6`
    """
    df.createOrReplaceTempView("Hotel")
    return spark.sql("Select * from Hotel where Review_Total_Positive_Word_Counts >= Review_Total_Negative_Word_Counts AND Reviewer_Score >= 6")

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
    dfCopia = df
    if modality == 1:
        dfCopia = (df.groupby(df.Reviewer_Nationality)
                   .avg("Reviewer_Score")
                   .sort("avg(Reviewer_Score)", ascending=False))
    elif modality == 2:
        dfCopia = (df.groupby(df.Reviewer_Nationality).count()
                   .withColumnRenamed("count", "Totale recensioni")
                   .sort("Totale recensioni", ascending=False))
    elif modality == 3:
        dfCopia = df.groupby("Reviewer_Nationality") \
            .agg(
            F.sum("Review_Total_Positive_Word_Counts").alias("Total_Positive"),
            F.sum("Review_Total_Negative_Word_Counts").alias("Total_Negative")
        ) \
            .withColumn("Totale numero parole", F.col("Total_Positive") + F.col("Total_Negative")) \
            .sort("Totale numero parole", ascending=False)
    return dfCopia

def top_hotel_rev(modality: int):
    """
    :param modality:
        1 for top about hotel for mean of avg_score,
        2 for top about hotel with most number of review,
        3 for top about hotel with most review words counts.
    """
    dfCopia=df
    if modality == 1:
        dfCopia= df.groupby(df.Hotel_Name).avg("Average_Score").sort("avg(Average_Score)", ascending=False)

    elif modality == 2:
        dfCopia= df.groupby(df.Hotel_Name).count().withColumnRenamed("count","Totale recensioni").sort("Totale recensioni", ascending=False)

    elif modality == 3:
        dfCopia = df.groupby(df.Hotel_Name) \
            .agg(
            F.sum("Review_Total_Positive_Word_Counts").alias("Total_Positive"),
            F.sum("Review_Total_Negative_Word_Counts").alias("Total_Negative")
        ) \
            .withColumn("Totale numero parole", F.col("Total_Positive") + F.col("Total_Negative")) \
            .sort("Totale numero parole", ascending=False)

    #TODO
    elif modality == 4:
        dfCopia = df.groupby(df.Hotel_Name) \
            .agg(
            F.mean("Reviewer_Score").alias("mean"),
            F.stddev("Reviewer_Score").alias("stddev")
        )
        collections = dfCopia.collect()
        for row in collections :
            mean = row["mean"]
            stddev = row["stddev"]

            # Limiti dai dalla  regola empirica (o regola 68-95-99.7) della statistica applicata alla distribuzione normale (gaussiana).
            lower_bound = mean - 3 * stddev
            upper_bound = mean + 3 * stddev

            #Nuovo df con solo recensioni di quell'hotel
            dfHotel = df.filter(df.Hotel_Name == row["Hotel_Name"])
            ## Filtra gli outlier
            outliers = dfHotel.filter((F.col("Reviewer_Score") < lower_bound) | (F.col("Reviewer_Score") > upper_bound)).count()

            dfCopia = dfCopia.withColumn("outliers", F.when(dfCopia["Hotel_Name"] == row["Hotel_Name"],outliers))
    return dfCopia

def plot_reviews_by_dimension(Hotel_Name: str, Nationality: str):
    """
        Plotting number of reviews (y) by data (x) filtered by hotel, nazionality
    """
    dfCount=df.groupby(df.Review_Date).count().withColumnRenamed("count","Rev_For_Date")
    if Hotel_Name is not None:
        dfCount = dfCount.filter(dfCount.Hotel_Name == Hotel_Name)
    elif Nationality is not None:
        dfCount = dfCount.filter(dfCount.Nationality == Nationality)
    return dfCount.select("Rev_For_Date","Review_Date")

def filter_reviews_by_word(word: str):
    """
    Filtra le recensioni che contengono una determinata parola.

    :param word: La parola da cercare nelle recensioni.
    :return: Un DataFrame PySpark con le recensioni filtrate.
    """
    # Filtra le recensioni positive o negative che contengono la parola
    filtered_df = df.filter(
        (F.col("Positive_Review").contains(word)) |
        (F.col("Negative_Review").contains(word))
    )
    return filtered_df

def best_tags(df):
    """
    :return:  Un DataFrame PySpark con i tags ordinati per numero di utilizzi Tag-Count.
    """
    # Conversione della colonna Tags in un array TODO da inserire nell'aggiustamento dello schema? o lasciare come prima per il resto?
    dfNew = df.withColumn("Tags_Cleaned", F.regexp_replace(F.col("Tags"), r"[\[\]']", ""))
    dfNew = dfNew.withColumn("Tags_Array", F.split(F.col("Tags_Cleaned"), ",\s*"))
    dfNew = dfNew.withColumn("Tags_Array", F.expr("transform(Tags_Array, x -> trim(x))"))

    #explode separa ogni elemento dell'array in una nuova riga
    df_exploded = dfNew.withColumn("Tag", F.explode(F.col("Tags_Array")))

    tag_count = df_exploded.groupBy("Tag").count().orderBy("count", ascending=False)

    return tag_count

# FUNZIONI UTILI PER FILTERING

def all_nationality():
    return  df.select("Reviewer_Nationality").distinct().collect()

def all_Hotel_Name():
    return df.select("Hotel_Name").distinct().collect()


