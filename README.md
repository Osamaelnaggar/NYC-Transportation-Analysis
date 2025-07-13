# 🚦 NYC Transportation Analysis – Capstone Project

This project is the final capstone for the German University in Cairo's Data Engineering & AI Diploma. It provides a comprehensive, end-to-end data pipeline and analytics platform for analyzing multi-modal urban transportation in New York City, using real open-source datasets.

The project covers:
- Taxi, Bus, and Subway data sources
- ETL pipeline using Apache Airflow
- PostgreSQL as a data warehouse
- Containerized environment using Docker
- Machine Learning model for fare prediction
- Dashboards created with Apache Superset

---

## 📊 Project Highlights

- 🔄 Fully automated pipeline: ingest, clean, transform, and load public datasets  
- 🧠 ML-powered predictions on trip duration/fare using FFNN  
- 🧰 Dockerized infrastructure for reproducibility  
- 📈 Dashboards showing trends in urban mobility across time and space  

---

## 🏗️ Architecture & Tools

This project uses a modern data stack:

- **Apache Airflow** – Orchestration of ETL workflows  
- **PostgreSQL** – Structured data warehouse  
- **Docker** – Environment management & deployment  
- **Apache Superset** – Dashboard and BI layer  
- **Python** – Data cleaning, processing, and ML  
- **PySpark** – Big data processing for e-commerce modeling  
- **Jupyter Notebooks** – EDA and ML model training  

---

## 📁 Project Structure



```
├── Infra/                  # Docker setup and infra structure
├── Bus_BusLanes/              # Code for Bus and bus lanes datasets
├── Subway/                  # Code for Subway dataset
├── Taxi/                  # Code for Taxi dataset
├── ML_Model/              # Machine learning Model code for fare predication
├── Airflow_pipelines/          # Dags and python scripts for datasets pipelines
├── Sample_data/              # Data samples to be used with Airflow pipeline dags
├── Capstone Project-NYC Transportation Analysis.pptx              # Small presentation for the project and some analytics
└── README.md
```

## 🛠️ How to Run

1. Clone the repository
2. Ensure Docker is installed
3. From Docker folder Run (add build as bash parameter to rebuild containers):

<pre> ```bash # Initialize Postgress and Airflow services bash init.sh build # Initialize Superset bash init_superset.sh ``` </pre>

4. Access Airflow at: `http://localhost:8080`
5. Access Superset at: `http://localhost:8088`

## 🧠 Machine Learning

We trained models on Taxi data to predict trip fare using:
- Deep Learning FFNN Model

## 📊 Dashboards

Notebooks code provide key insights to be used on superset dashboard:
- Trip durations over time
- Day vs. night ride trends

## 👥 Authors

<<<<<<< HEAD
- **Adham Abdelhameed ElSharkawy** – Cloud Solution Architect - Microsoft
- **Osama ElNaggar** - Data Management Lead - Dana Petroleum
- **Abdullah Kamal** - Senior Data Steward - Schneider Electric
- **Ahmed Abdullah** - 	Senior Data Analyst - Seoudi Corporate
- **Ajeeb ElAmeen** - Senior data Analyst

>>>>>>> 081a298a9e8f29b916eedbc2686da66679ff2906

## 📜 License

MIT License

## 🌐 Public Datasets Used

- [NYC Traffic Commission Data](https://www.nyc.gov/html/dot/html/about/datafeeds)
                               (https://catalog.data.gov/dataset/mta-subway-stations)
                               (https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

---

> This project aims to serve as a reusable and extendable framework for urban mobility and traffic analysis. Contributions welcome!
<<<<<<< HEAD
=======

>>>>>>> 081a298a9e8f29b916eedbc2686da66679ff2906
