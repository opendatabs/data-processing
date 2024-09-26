# Aggregation Process

This document outlines the data aggregation process for this project.

## Data Sources and Aggregation:

1. **Data up to quarter 2024-1**:
   - The data comes pre-aggregated from the **Stadtreinigung** in Excel format.

2. **Data from quarter 2024-2 onwards**:
   - The aggregation is performed by the **Statistisches Amt**. 
   - Below, the detailed process is described.

## Aggregation Process (from 2024-2 onwards):

1. **Data Download**:
   - First, the data from [Sauberkeitsindex pro Monat und Strassenabschnitt](https://data.bs.ch/explore/dataset/100288/) is downloaded.

2. **Filtering Complete Quarters**:
   - Complete quarters are determined. These are quarters where data for all three months is available.
   - Quarters with incomplete data (where not all three months are present) are ignored.

3. **Centroid Calculation**:
   - The centroid of each street section is determined.

4. **Wohnquartier Data**:
   - Data for the **Wohnquartier** is loaded from [Statistische Raumeinheiten: Wohnviertel ](https://data.bs.ch/explore/dataset/100042/).
   - **Riehen** and **Bettingen** are excluded, as they do not appear in the Excel file from data up to 2024-1 either.

5. **Assigning Centroids to Wohnquartiere**:
   - For each centroid, the **Wohnquartier** containing it is identified.
   - If no **Wohnquartier** contains the centroid, the closest **Wohnquartier** is determined.

6. **Calculating Average CCI**:
   - The average **Sauberkeitsindex** (called `CCI` in [Sauberkeitsindex pro Monat und Strassenabschnitt](https://data.bs.ch/explore/dataset/100288/), and `SKI` in our aggregated data) for all measurements in that quarter is calculated, referred to as **"gesamtes Stadtgebiet"**.
   - Additionally, the average **SKI** is calculated separately for each **Wohnquartier**.
