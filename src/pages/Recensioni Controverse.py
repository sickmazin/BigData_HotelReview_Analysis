import streamlit as st
from allQuery import all_Hotel_Name, controversial_reviews

#controversial_reviews
hotel_names= all_Hotel_Name()
col1, col2, col3, col4 = st.columns(4)
hotel3 = col1.selectbox("Select hotel", hotel_names, format_func=lambda x: "None" if x is None else x, key="hotel3")
param = col2.number_input("Insert parameter", value=3.4, step=0.1)
positive = col3.selectbox("Positive", [None, "Positive", "Negative"], format_func=lambda x: "None" if x is None else x)
ascending_cb = col4.checkbox("Ascending", value=False)

controversial_reviews = controversial_reviews(positive=positive, param=param, hotel=hotel3).toPandas()

controversial_reviews.sort_values(by="Score", ascending=ascending_cb)
st.write(controversial_reviews)