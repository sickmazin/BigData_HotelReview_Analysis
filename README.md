# Big Data Analysis: Luxury Hotel Reviews

## Descrizione del Progetto
Questo progetto si concentra sull'analisi di recensioni relative a hotel di lusso in Europa, sfruttando un dataset contenente 515.000 recensioni provenienti da Booking.com. L'obiettivo è fornire insight dettagliati per migliorare i servizi alberghieri, identificare tendenze culturali e creare una dashboard interattiva per la visualizzazione dei risultati.

La dashboard è sviluppata utilizzando [Streamlit](https://streamlit.io/), che consente la creazione di applicazioni web interattive per visualizzare e analizzare dati in modo intuitivo. Apache Spark, attraverso l'API PySpark, è stato utilizzato per gestire il volume e la varietà dei dati.

## Caratteristiche del Progetto
- **Analisi del Dataset**: Comprende la sentiment analysis, clustering, e l'analisi delle correlazioni.
- **Dashboard Interattiva**: Permette di esplorare:
  - Recensioni positive e negative.
  - Distribuzione geografica degli hotel su mappe interattive.
  - Grafici come word cloud, istogrammi e grafici a torta.
- **Sentiment Analysis**: Implementata utilizzando modelli di regressione logistica e SVM con tecniche avanzate di NLP.
- **Preprocessing Avanzato**:
  - Conversione e pulizia di colonne come date, punteggi e tag.
  - Rimozione di valori mancanti.
  - Salvataggio dei dati in formato parquet per ottimizzare le prestazioni.

## Requisiti
Assicurati di avere installato le seguenti dipendenze prima di eseguire il progetto:
```plaintext
pyspark~=3.5.4
pandas==2.2.3
streamlit~=1.41.1
streamlit_folium~=0.24.0
folium~=0.19.4
wordcloud~=1.9.4
matplotlib~=3.10.0
pyspark-stubs~=3.0.0.post3
plotly~=5.24.1
```
Puoi installare tutte le dipendenze utilizzando il comando:
```bash
pip install -r requirements.txt
```
# Dataset
Il dataset utilizzato è disponibile su Kaggle e contiene:
 - Nome e indirizzo degli hotel.
 - Recensioni positive e negative.
 - Nazionalità dei recensori.
 - Punteggi assegnati.
 - Tag e date di recensione.

Fonte del dataset: [515k Hotel Reviews Data in Europe](kaggle.com/datasets/jiashenliu/515k-hotel-reviews-data-in-europe)

## Dashboard
La homepage della dashboard include:
Mappa Interattiva: Mostra la posizione degli hotel.
Word Cloud: Visualizza le parole più frequenti nelle recensioni positive e negative.
Grafici a Torta: Rappresentano proporzioni e distribuzioni chiave.
Filtri Personalizzati: Permettono di esplorare recensioni basate su hotel, nazionalità e altri parametri.

### Sviluppato da
Mattia Coriglia e Paolo Costa per l'esame di Big Data, anno 2025




