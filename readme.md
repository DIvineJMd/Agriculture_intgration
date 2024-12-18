# Smart Agriculture Advisor

## Overview
The Smart Agriculture Advisor project is an innovative system designed to empower farmers and agricultural stakeholders with actionable insights. By integrating multiple data sources, processing them through an advanced ETL pipeline, and presenting the results via a user-friendly application, the system offers features such as fertilizer recommendations, crop selection advice, market analysis, and a chatbot advisor.

---

## Features

### Fertilizer Recommendation
- Analyzes soil health data specific to your district.
- Suggests optimal fertilizers to enhance crop yield.

### Crop Recommendation
- Provides crop suggestions based on:
  - Upcoming weather conditions.
  - Soil health metrics.
  - Irrigation capabilities.

### Market Analysis
- Delivers profit-maximizing crop recommendations using:
  - Historical crop price data.
  - Regional market trends.

### Market Intelligence System
- Offers live market analysis views, combining real-time and historical data.
- Helps farmers track price fluctuations and demand patterns for crops.

### Chatbot Advisor
- Powered by the NVIDIA LLaMA model.
- Executes federated queries on a Google Cloud Platform (GCP) database to answer user questions.

---

## Data Sources

1. **Open Meteo Weather API**
   - Provides real-time and forecasted weather data.

2. **Soil Health Data**
   - Scrapes soil health metrics from the Indian Government Soil Health Portal.

3. **Kaggle Datasets**
   - **Irrigation Data**
   - **Crop Price Data**
   - **Crop Requirement Data**
   - **Fertilizer Recommendation Data**

4. **Google Cloud Storage**
   - Hosts static datasets to simulate live data fetching.

---

## Architecture

### ETL Pipeline
- **Extract**: Gathers data from APIs, web scraping, and static datasets stored in Google Cloud.
- **Transform**: Cleans and processes data to ensure consistency and usability.
- **Load**: Saves the transformed data in a federated GCP database for efficient querying.

### Application
- Provides an intuitive user interface for accessing recommendations and insights.
- Features modular components for each functionality.

### Chatbot
- Utilizes NVIDIA LLaMA for natural language processing.
- Dynamically connects to the GCP database to generate and execute queries.

---

## Tools and Technologies

- **Google Cloud Platform (GCP)**: Hosts data storage and supports federated querying.
- **Open Meteo API**: Supplies weather data.
- **Web Scraping**: Extracts soil health information.
- **Kaggle**: Offers static datasets for analysis.
- **ETL Pipeline**: Built with Python and cloud tools.
- **NVIDIA LLaMA Model**: Implements advanced language capabilities for the chatbot.
- **Streamlit/Django/Flask**: Frameworks used for web application development (specify the one implemented).

---

