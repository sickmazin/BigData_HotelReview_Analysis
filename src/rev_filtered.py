import streamlit as st
from main import base_filtering, all_nationality, all_Hotel_Name

st.title("Visualizzazione estesa delle recensioni")
st.markdown("")
st.write("Qui puoi applicare diversi filtri per visualizzare recensioni specifiche degli hotel. Puoi selezionare un hotel, una nazionalità del recensore, e impostare filtri sui punteggi delle recensioni e dei punteggi medi degli hotel. Inoltre, puoi scegliere di visualizzare recensioni positive e/o negative, e filtrare le recensioni in base a una data specifica."
         " I risultati vengono mostrati in un formato tabellare con informazioni dettagliate per ciascuna recensione.")
st.markdown("---")

st.subheader("Filtraggio")

def create_filter_ui():
    # Organizza i filtri in colonne
    col1, col2 = st.columns(2)

    # Filtraggio per il nome dell'hotel
    with col1:
        filter_Name = st.selectbox(
            "Seleziona l'Hotel:",
            options= [None] + sorted(all_Hotel_Name())
        )
    # Filtraggio per nazionalità del recensore
    with col2:
        filter_Nationality_Rev = st.selectbox(
            "Seleziona nazionalità delle recensioni:",
            options=[None] + sorted(all_nationality())[1:]
        )
    # Filtraggio per il punteggio della recensione
    with col1:
        st.write("**Filtra per punteggio della recensione:**")
        coll11,coll12 = st.columns(2)
        with coll11:
            filter_Rev_Score_value = st.number_input("Punteggio minimo:", min_value=0.0, max_value=10.0, value=0.0, step=0.1)
        with coll12:
            filter_Rev_Score_operator = st.selectbox("Operatore:", ["Maggiore di", "Minore di"], index=0, key="rev_score_operator")
        if filter_Rev_Score_operator == "Maggiore di":
            filter_Rev_Score = (filter_Rev_Score_value, ">")
        else:
            filter_Rev_Score = (filter_Rev_Score_value, "<")

    # Filtraggio per il punteggio medio
    with col2:
        st.write("**Filtra per punteggio medio dell'hotel:**")
        col11,col12 = st.columns(2)
        with col11:
            filter_Avg_Score_value = st.number_input("Punteggio medio minimo:", min_value=0.0, max_value=10.0, value=0.0, step=0.1)
        with col12:
            filter_Avg_Score_operator = st.selectbox("Operatore:", ["Maggiore di", "Minore di"], index=0, key="avg_score_operator")
        if filter_Avg_Score_operator == "Maggiore di":
            filter_Avg_Score = (filter_Avg_Score_value, ">")
        else:
            filter_Avg_Score = (filter_Avg_Score_value, "<")

    # Organizza altri filtri in due nuove colonne
    col1, col2 = st.columns(2)

    # Filtraggio per recensioni positive
    with col1:
        filter_Positive_Rev = st.checkbox("Mostra recensioni positive", value=True)
        filter_Negative_Rev = st.checkbox("Mostra recensioni negative", value=True)

    # Filtraggio per la data delle recensioni
    with col2:
        filter_Date = st.date_input("Data limite per le recensioni:", value=None)
        word_to_search = st.text_input("Cerca recensioni per parola:")

    return filter_Name, filter_Rev_Score, filter_Avg_Score, filter_Nationality_Rev, filter_Negative_Rev, filter_Positive_Rev, filter_Date, word_to_search

# Uso della funzione
filter_Name, filter_Rev_Score, filter_Avg_Score, filter_Nationality_Rev, filter_Negative_Rev, filter_Positive_Rev, filter_Date,word_to_search = create_filter_ui()

data = (base_filtering(
    filter_Name,
    filter_Rev_Score,
    filter_Avg_Score,
    filter_Nationality_Rev,
    filter_Negative_Rev,
    filter_Positive_Rev,
    filter_Date,
    word_to_search
).toPandas().to_dict(orient='records'))  # Trasforma in una lista di dizionari



st.markdown("---")
st.subheader("Recensioni")
st.markdown("---")


num_reviews = len(data)
reviews_per_page = 10
total_pages = (num_reviews // reviews_per_page) + (1 if num_reviews % reviews_per_page > 0 else 0)

if 'current_page' not in st.session_state:
    st.session_state.current_page = 1

col1, col2, col3 = st.columns([1, 6, 1])
with col1:
    if st.session_state.current_page >= 2:
        if st.button("Precedente"):
            st.session_state.current_page -= 1
with col3:
    if st.session_state.current_page < total_pages:
        if st.button("Successivo"):
            st.session_state.current_page += 1

start_idx = (st.session_state.current_page - 1) * reviews_per_page
end_idx = start_idx + reviews_per_page


# Visualizzare le recensioni per la pagina selezionata
st.markdown(f"### Pagina {st.session_state.current_page} di {total_pages}")
for i, row in enumerate(data[start_idx:end_idx]):
    # Colonne per Hotel Name e Address
    col1, col2 = st.columns(2)
    with col1:
        st.write(f"🏨 **{row.get('Hotel_Name')}**")
    with col2:
        st.write(f"📍 **{row.get('Hotel_Address')}**")
    st.markdown("")

    if filter_Positive_Rev:
        st.write(f"✅ {row.get('Positive_Review')}")
    if filter_Negative_Rev:
        st.write(f"❌ {row.get('Negative_Review')}")
    st.markdown("")

    # Colonne per ulteriori dettagli
    col1, col2, col3 = st.columns(3)
    with col1:
        st.write(f"🗓️ **Date:** {row.get('Review_Date')}")
    with col2:
        st.write(f"🌍 **Reviewer Nationality:** {row.get('Reviewer_Nationality')}")
    with col3:
        st.write(f"⭐ **Score:** {row.get('Reviewer_Score')} 🌟")
    st.markdown("---")

