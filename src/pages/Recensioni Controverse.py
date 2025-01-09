import streamlit as st

from allQuery import all_Hotel_Name, controversial_reviews

st.subheader("Query per Recensioni Controverse")
st.markdown(f"""
Questa query identifica le recensioni più controverse, ovvero quelle il cui punteggio differisce significativamente dalla media dell'hotel (con una soglia iniziale di **3.4 punti**, ma che può esser variata).  
È utile per individuare opinioni fuori dal comune o potenziali anomalie nel feedback degli utenti.
""")

#controversial_reviews
hotel_names= all_Hotel_Name()
col1, col2, col3, col4 = st.columns(4)
hotel3 = col1.selectbox("Seleziona hotel", hotel_names, format_func=lambda x: "None" if x is None else x, key="hotel3")
param = col2.number_input("Valore soglia", value=3.4, step=0.1)
positive = col3.selectbox("Tipologia", [None, "Positive", "Negative"], format_func=lambda x: "None" if x is None else x)
ascending_cb = col4.checkbox("Ascending", value=False)


controversial_reviews = controversial_reviews(positive=positive, param=param, hotel=hotel3).orderBy("Score", ascending=ascending_cb).toPandas()

count = sum(1 for _ in controversial_reviews.iterrows())
if count==0:
    st.markdown("---")
    st.markdown("Non ci sono recensioni controverse con questo valore di soglia per questo Hotel")
for i, row in controversial_reviews.iterrows():
    col1, col2 = st.columns([1,8])
    with col1:
        st.write(f"⭐ **Score:** {round(row.get('Score'), 2)}")
    with col2:
        st.write(f"📄 {row.get('Review')}")
    st.markdown("---")
