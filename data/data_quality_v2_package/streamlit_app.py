
import streamlit as st
import pandas as pd

st.set_page_config(layout="wide")

st.title("📊 Data Quality Dashboard")

st.sidebar.header("Filtros")
dataset = st.sidebar.text_input("Dataset")

st.subheader("Resumo Executivo")
st.metric("Score Geral", "8.2", "+0.3")

st.subheader("Radar por Dimensão")
st.write("Radar chart será implementado aqui")

st.subheader("Histórico")
st.line_chart(pd.DataFrame({
    "score": [7.5, 7.8, 8.0, 8.2]
}))
