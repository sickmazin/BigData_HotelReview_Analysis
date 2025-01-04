import streamlit as st
import plotly.express as px
from main import best_tags, topDict, all_Hotel_Name, all_nationality, plot_reviews_by_dimension, \
    all_reviews_score_by_Hotel, top_hotel_rev, top_nationality_rev, top_rev_score

data = topDict(best_tags(),15)
fig = px.histogram(data, x="Tag", y="count", nbins=20, title="Distribuzione dei tags nelle recensioni",
                   labels={"Year": "Anno", "Review_Count": "Conteggio delle recensioni"})
fig.update_layout(
    barmode='group',  # Gruppo le barre
    xaxis_title="Tags",  # Titolo dell'asse X
    yaxis_title="Numero di apparizioni",  # Titolo dell'asse Y
)

st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

st.subheader("Distribuzione delle recensioni nel tempo")
col1, col2 = st.columns(2)
with col1:
    filter_Name = st.selectbox(
        "Seleziona l'Hotel:",
        options= [None] + sorted(all_Hotel_Name())
    )
with col2:
    filter_Nationality = st.selectbox(
        "Seleziona nazionalità delle recensioni:",
        options=[None] + sorted(all_nationality())[1:]
    )
data2 = topDict(plot_reviews_by_dimension(filter_Name, filter_Nationality),20)
fig = px.histogram(data2, x="Review_Date", y="Rev_For_Date",nbins=10, labels={"Review_Date": "Data", "Rev_For_Date": "Conteggio delle recensioni"})
fig.update_layout(
    barmode='group',  # Gruppo le barre
    xaxis_title="Data",  # Titolo dell'asse X
    yaxis_title="Numero di recensioni",  # Titolo dell'asse Y
)

st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

st.subheader("Distribuzione delle recensioni per voto di un hotel specifico")
all_Hotel_Name=all_Hotel_Name()
filter_Hotel = st.selectbox(
    "Seleziona l'Hotel:",
    options= [None] + sorted(all_Hotel_Name),
    key="Hotel_Name_Box",
    index=1,
)
@st.cache_data
def data3(filter_Hotel):
    return all_reviews_score_by_Hotel(filter_Hotel).toPandas()

data3= data3(filter_Hotel)

col1, col2 = st.columns([2,1])
if not data3.empty:
    fig = px.box(
        data3,
        y="Reviewer_Score",
        title=f"Box Plot dei punteggi per {filter_Hotel}",
        points="all",
        labels={"Review Score": "Punteggi delle recensioni"},
        template="plotly_white"
    )
    with col1:
        st.plotly_chart(fig,use_container_width=False)
    with col2:
        st.markdown(" ")
        st.markdown(" ")
        st.write("Attraverso questo grafico: il **box plot**, si riassume visivamente la distribuzione dei voti delle recensioni relative all'hotel, mostrando i valori centrali (mediana) e la variabilità (quartili)."
                 " È utile per individuare rapidamente gli **outlier**, rappresentati come punti fuori dai \"baffi\".")
else:
    st.write("Seleziona un hotel per visualizzare il grafico.")


# PIE CHART

@st.cache_data
def getDict():
    list=[]
    data_nat1 = topDict(top_nationality_rev(1),10)
    data_nat3 = topDict(top_nationality_rev(3),10)
    data_hot1 = topDict(top_hotel_rev(1),10)
    data_hot3 = topDict(top_hotel_rev(3),10)
    list.append(data_nat1)
    list.append(data_nat3)
    list.append(data_hot1)
    list.append(data_hot3)
    return list

pieChartListOfDict=getDict()

# Creazione dei pie chart con Plotly
fig_nat1 = px.histogram(pieChartListOfDict[0], x='Reviewer_Nationality', y='avg(Reviewer_Score)', title="Classifica delle nazionalità in base alla media del punteggio dei recensori")
fig_nat3 = px.histogram(pieChartListOfDict[1], x='Reviewer_Nationality', y='Totale numero parole', title="Top 10 Nazionalità più logorroiche")
fig_hot1 = px.pie(pieChartListOfDict[2], names='Hotel_Name', values='Totale recensioni', title="Top 10 Hotel per media score alta")
fig_hot3 = px.pie(pieChartListOfDict[3], names='Hotel_Name', values='Totale numero parole', title="Top 10 Hotel per numero di parole nelle recensioni")

# Layout a due colonne per i primi due pie chart
col1, col2 = st.columns(2)

with col1:
    st.plotly_chart(fig_nat1, use_container_width=True)
    st.plotly_chart(fig_hot1, use_container_width=True)  # Aggiungi il secondo pie chart degli hotel

with col2:
    st.plotly_chart(fig_nat3, use_container_width=True)
    st.plotly_chart(fig_hot3, use_container_width=True)  # Aggiungi il pie chart delle recensioni
