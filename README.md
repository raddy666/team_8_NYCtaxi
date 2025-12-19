# 🚕 Taxi Big Data Analysis & Prediction System

A **distributed big data analytics and prediction system** designed to analyze large-scale taxi transportation data and provide **demand hotspot analysis, congestion insights, and real-time wait time prediction**.

This project was developed during my **Software Engineering Internship (Big Data Analysis Team)** and covers **end-to-end data engineering**, from raw data ingestion to distributed processing, analytics, visualization, and AI-assisted prediction.

---

## 📌 Project Overview

The system processes large volumes of taxi trip data using a **Hadoop-based distributed environment**, performs analytics with **Spark and Hive**, visualizes insights via **Apache Zeppelin**, and exposes predictions through a **ChatGPT-integrated assistant**.

Key goals:
- Scalable data processing on distributed infrastructure
- Reliable ETL pipeline for transportation datasets
- Actionable analytics for urban mobility
- Real-time prediction using historical patterns

---
### 📊 Dataset Scale

- **City:** New York City  
- **Time Span:** 2010 – 2013  
- **Data Size:** ~3.15 GB (3,302,447 KB)  
- **Records:** Approximately 20–25 million taxi trip records  

The dataset captures multi-year urban transportation patterns at city scale, enabling large-scale demand analysis and congestion modeling.


## 🏗️ System Architecture

```text
Raw Taxi Data (Parquet)
        ↓
ETL Pipeline (PySpark)
        ↓
HDFS Distributed Storage
        ↓
Apache Hive (SQL Analytics)
        ↓
Apache Zeppelin (Visualization)
        ↓
Prediction Layer (ChatGPT + Historical Analysis)
```
---

## 🔧 Core Features

### 📊 Distributed Data Processing
- 3-node Hadoop cluster with dedicated master node
- HDFS for fault-tolerant distributed storage
- Spark-based batch processing for large datasets

### 🔄 ETL Pipeline
- Parquet → CSV data transformation
- Data cleaning and normalization using PySpark
- Structured storage in MySQL for downstream access
- Automated data flow from ingestion to analytics

### 📈 Analytics & Visualization
- 30+ interactive visualizations in Apache Zeppelin
- Geospatial heatmaps for taxi demand hotspots
- Congestion pattern analysis using Hive SQL queries
- Time-based and region-based demand trends

### 🤖 Prediction & AI Integration
- ChatGPT-integrated prediction bot
- query-time prediction based on historical patterns
- Area-based congestion insights derived from historical data
- Natural language interface for querying analytics results

---

## 🧠 Engineering Responsibilities

- System architecture design and end-to-end pipeline ownership
- Hadoop cluster administration on Linux
- ETL pipeline design and implementation
- Spark and Hive query optimization
- Design and implementation of analytical dashboards for decision support
- AI-assisted prediction system integration

---

## 🛠️ Technology Stack

- **Big Data:** Hadoop (HDFS, MapReduce)
- **Processing:** Apache Spark, PySpark
- **Query Engine:** Apache Hive
- **Visualization:** Apache Zeppelin
- **Database:** MySQL
- **Programming:** Python (Pandas, PySpark)
- **Platform:** Linux
- **IDE:** PyCharm
- **AI Integration:** OpenAI API (ChatGPT)

---

## ▶️ Setup & Execution (Local / Cluster)

>  This project targets a Hadoop/Spark cluster environment and is not intended for single-machine execution without distributed setup.


### Environment Setup
- Linux
- Hadoop cluster (standalone or multi-node)
- Apache Spark
- Apache Hive
- Apache Zeppelin
- MySQL
- Python 3.x

### Typical Workflow
1. Load raw taxi data into HDFS
2. Run PySpark ETL scripts
3. Execute Hive queries for analytics
4. Visualize results in Zeppelin
5. Query predictions via AI assistant

---

## 📈 Example Use Cases

- Identifying taxi demand hotspots by time and location
- Detecting congestion-prone zones in urban areas
- Estimating taxi wait times at query-time
- Supporting data-driven transportation planning

---

## 🚧 Possible Extensions

- Real-time streaming using Kafka
- Deep learning-based demand forecasting
- REST API for prediction services
- City-scale deployment and performance benchmarking
- Dashboard deployment for municipal use

---

## 👤 Author

**MD Tahmid Hamim**  
Software Engineering Intern – Big Data Analysis Team  
Chengdu Suncape Data Co., Ltd. (March 2025 – September 2025)

---

## 🏅 Internship Outcome

- Certificate of Completion with **excellent evaluation**
- Successfully delivered production-grade big data analytics system
