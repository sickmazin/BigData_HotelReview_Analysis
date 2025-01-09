import folium
import streamlit as st
from streamlit_folium import st_folium
from allQuery import number_of_Hotel, top_hotel_rev, top_nationality_rev, topDict, top_rev_score, review_plot_yearORmonths, \
    all_Hotel_Name, dataMap
import plotly.express as px
import matplotlib.pyplot as plt
import allQuery as u
import streamlit as st
import folium
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
from streamlit_folium import st_folium
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, StopWordsRemover
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from wordcloud import WordCloud
import matplotlib.pyplot as plt

@st.cache_data
def get_dataMap():
    return dataMap()

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
def get_wordclouds():
    _wc_review=  u.get_Rev_Score()
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

st.title("Hotel Review Analytics")
st.write("")

# MAPPA
st.header("Mappa")

#center_eu = [54.5260, 15.2551]
#map = folium.Map(location=center_eu, zoom_start=5, prefer_canvas=True, tiles="cartodb positron")
#marker_cluster = folium.plugins.MarkerCluster().add_to(map)
#
#dfMap = get_dataMap()
#
#
#for row in dfMap.itertuples():
#    folium.Marker([row.lat, row.lng], popup=folium.Popup(popup(row.Hotel_Name, row.Reviews, round(row.Average_Score, 1)), max_width=250)).add_to(marker_cluster)
#
#st_folium(map, width=700, height=500)



### Sezione di introduzione
st.markdown("---")
st.subheader("Perché Analizzare Questo Dataset?")
st.write(
    "Nel mondo sempre più competitivo dell'ospitalità di lusso, comprendere l'esperienza del cliente è fondamentale per migliorare i servizi e fidelizzare i clienti. "
    "Questo dataset, estratto da Booking.com, rappresenta una straordinaria opportunità per approfondire il mondo degli hotel di lusso in Europa attraverso l'analisi di **515.000 recensioni** lasciate da ospiti provenienti da tutto il mondo.\n "
)
st.write(
    "Recensioni relative a **"+str(number_of_Hotel())+"** Hotel sparsi in tutto il mondo."
)

col1, col2 = st.columns(2)
# Descrizione del dataset
with col1:
    st.header("Cosa Contiene il Dataset?")
    st.markdown(
        "Il dataset include informazioni dettagliate sulle recensioni e sugli hotel, come:")
    fields = [
        "Indirizzo e Nome dell'Hotel",
        "Recensioni Positive e Negative",
        "Nazionalità dei Recensori",
        "Punteggi assegnati",
        "Tags e Tempi di Recensione",
    ]
    for field in fields:
        st.write(f"- {field}")

# Possibili analisi
with col2:
    st.subheader("Quali Analisi Possiamo Fare?")
    st.markdown(
            "Questo dataset è un vero tesoro di possibilità:")

    analyses = [
    "**Sentiment Analysis:** Scopri quali parole e frasi influenzano maggiormente le valutazioni degli ospiti.",
    "**Correlazioni:** Analizza il legame tra la nazionalità dei recensori e i punteggi assegnati.",
    "**Clustering e Raccomandazioni:** Raggruppa gli hotel per caratteristiche comuni o crea un motore di raccomandazione per clienti con preferenze specifiche.",
    "**Visualizzazioni Informative:** Crea mappe e grafici per rappresentare tendenze e insight in modo accattivante."
]
    for analysis in analyses:
            st.write(f"- {analysis}")

st.subheader("Perché È Importante?")
st.write(
    "Capire cosa rende un soggiorno \"indimenticabile\" è il primo passo per offrire un'esperienza straordinaria. "
    "Grazie a questo dataset, possiamo non solo ottimizzare i servizi degli hotel, ma anche migliorare l'esperienza dei viaggiatori, garantendo che ogni soggiorno sia all'altezza delle loro aspettative.🌟"
)
st.markdown("---")

#CLOUD WORD

p_wordcloud, n_wordcloud = get_wordclouds()
col1, col2 = st.columns(2)
with col1:
    st.subheader("Word Cloud delle recensioni positive")
with col2:
    st.subheader("Word Cloud delle recensioni negative")

fig, axes = plt.subplots(1, 2, figsize=(12, 6), facecolor='none')

axes[0].imshow(p_wordcloud, interpolation='bilinear')
axes[0].axis('off')

axes[1].imshow(n_wordcloud, interpolation='bilinear')
axes[1].axis('off')

st.pyplot(fig)



# PIE CHART
st.markdown("---")
st.header("Grafici a torta")
@st.cache_data
def getDict():
    list=[]
    data_hotel = topDict(top_hotel_rev(2),10)
    data_reviews2 = topDict(top_rev_score(2),10)
    data_nation = topDict(top_nationality_rev(2),10)
    data_reviews = topDict(top_rev_score(1),10)
    list.append(data_hotel)
    list.append(data_nation)
    list.append(data_reviews)
    list.append(data_reviews2)
    return list

pieChartListOfDict=getDict()

# Creazione dei pie chart con Plotly
fig_hotel = px.pie(pieChartListOfDict[0], names='Hotel_Name', values='Totale recensioni', title="Top 10 Hotel per numero di recensioni")
fig_nation = px.pie(pieChartListOfDict[1], names='Reviewer_Nationality', values='Totale recensioni', title="Top 10 Nazionalità per numero di recensioni")
fig_hotel2 = px.pie(pieChartListOfDict[3], names='Review_Type', values='Totale recensioni', title="Recensioni Positive, Negative o Neutre")
fig_reviews = px.pie(pieChartListOfDict[2], names='Reviewer_Score', values='Totale recensioni', title="Top 10 Voti per numero di recensioni")

# Layout a due colonne per i primi due pie chart
col1, col2 = st.columns(2)

with col1:
    st.plotly_chart(fig_hotel, use_container_width=True)
    st.plotly_chart(fig_hotel2, use_container_width=True)  # Aggiungi il secondo pie chart degli hotel

with col2:
    st.plotly_chart(fig_nation, use_container_width=True)
    st.plotly_chart(fig_reviews, use_container_width=True)  # Aggiungi il pie chart delle recensioni

# HISTOGRAM
st.markdown("---")
st.header("Istogrammi")

dataHisto1 = topDict(review_plot_yearORmonths(2, None),12)

fig = px.histogram(dataHisto1, x="Year", y="Review_Count", nbins=4, title="Distribuzione delle recensioni negli anni",
                   labels={"Year": "Anno", "Review_Count": "Conteggio delle recensioni"})
fig.update_layout(
    barmode='group',  # Gruppo le barre
    xaxis_title="Anno",  # Titolo dell'asse X
    yaxis_title="Numero di recensioni",  # Titolo dell'asse Y
)

col1, col2 = st.columns([3, 1])

with col1:
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.markdown("""
    **Deduzione:**  
    Dal grafico possiamo osservare che il numero di recensioni per anno presenta un picco significativo nel 2016, seguito da una diminuzione nell'anno successivo. Questo potrebbe indicare un aumento di popolarità o visibilità delle strutture nel 2016, seguito da una stabilizzazione o un calo delle recensioni negli anni successivi. 
    Purtroppo non possiamo notare molto dell'andamento poiché le recensioni fanno riferimento soltanto a 3 anni specifici.
    """)


dataHisto2 = topDict(review_plot_yearORmonths(1, None),12 )
fig = px.histogram(dataHisto2, x="Month", y="Review_Count", nbins=20, title="Distribuzione delle recensioni nei mesi",
                   labels={"Year": "Anno", "Review_Count": "Conteggio delle recensioni"})
fig.update_layout(
    barmode='group',  # Gruppo le barre
    xaxis_title="Mesi",  # Titolo dell'asse X
    yaxis_title="Numero di recensioni",  # Titolo dell'asse Y
)

col1, col2 = st.columns([3, 1])

with col1:
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.markdown("""
    **Deduzione:**  
    Dal grafico osserviamo la distribuzione del numero di recensioni nei diversi mesi dell'anno per tutti gli anni. Si nota una certa uniformità nel numero di recensioni durante l'anno, con variazioni non troppo marcate. Tuttavia, alcuni mesi sembrano leggermente più ricchi di recensioni (ad esempio il mese 8), mentre altri mesi (come il mese 11) mostrano un lieve calo.
    Questo potrebbe indicare una stagionalità moderata, probabilmente legata ai periodi di vacanza o alta stagione per gli hotel.
    """)

