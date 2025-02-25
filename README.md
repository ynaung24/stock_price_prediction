# 🚀 MarketStack to MongoDB - Airflow DAG

This repository contains an **Apache Airflow DAG** that fetches **daily stock market data** from [MarketStack](https://marketstack.com/) at **5 PM UTC** and stores it in **MongoDB Atlas**.

Next Steps: 

1. 

## 📌 Features

✅ Automated scheduling with Airflow  
✅ Fetches stock data from MarketStack API  
✅ Stores data in MongoDB Atlas (Cloud database)  
✅ Logging for monitoring & debugging  

---

## 🚀 Setup Guide

## 1️⃣ Install Prerequisites

Ensure you have **Python 3.12 or later+** installed. Then install the required dependencies:

```bash
pip install apache-airflow pymongo requests pyyaml
```

## 2️⃣ Connecting to the MongoDB Cluster

This project **stores stock data in the** `makertdata` **MongoDB cluster**. Follow these steps to create a MongoDB Atlas account and connect to the cluster.

### 🔹 Step 1: Create a MongoDB Atlas Account

1. Go to **[MongoDB Atlas](https://www.mongodb.com/atlas)**.
2. Click **"Start Free"** and sign up using your email or GitHub.
3. After signing up, log in to **MongoDB Atlas**.

### 🔹 Step 2: Get Your MongoDB Connection URI

1. **Your MongoDB Connection URI** for this project:

```bash
   mongodb+srv://your_username:your_password@makertdata.iinrc.mongodb.net/?retryWrites=true&w=majority&appName=makertdata
```
2. Replace your_username and your_password with your actual credentials.

## 3️⃣ Configure Airflow

### 🔹 Step 1: Move the DAG File

Ensure your Airflow DAGs folder is set up:

```bash
mkdir -p ~/airflow/dags ~/airflow/config ~/airflow/logs
mv marketstack_to_mongo.py ~/airflow/dags/
```
### 🔹 Step 2: Configure API & Database in `config.yml`

Create `~/airflow/config/config.yml` and paste:

```yaml
marketstack:
  api_key: "your_marketstack_api_key"

mongodb:
  uri: "mongodb+srv://your_username:your_password@makertdata.iinrc.mongodb.net/stock_data"
  database: "stock_data"
  collection: "market_prices"
```
🔹 Replace your_marketstack_api_key, your_username, and your_password with your actual credentials.

## 4️⃣ Start Airflow

Run these commands:

```bash
airflow db init
airflow scheduler &
airflow webserver &
```

Go to http://localhost:8080, find marketstack_to_mongo, and enable it.

## 5️⃣ Verify Everything is Working

### ✅ Check DAG Execution

- **Trigger DAG manually** from Airflow UI.

### ✅ Check Logs

Run:

```bash
tail -f ~/airflow/logs/log_script.log
```

### ✅ Verify Data in MongoDB Cluster

Use **MongoDB Shell** or **MongoDB Compass** or **MongoDB Atlas**


## 🔧 Troubleshooting

| Issue                         | Solution  |
|--------------------------------|-----------|
| **DAG not appearing in Airflow UI** | Restart `airflow scheduler` & `airflow webserver` |
| **Import errors in DAG**       | Add `sys.path.append("~/Desktop/projects/dds_t11")` |
| **MongoDB connection failing** | Ensure your **MongoDB URI is correct** |
| **No data in MongoDB**         | Check `log_script.log` for API errors |


## 📌 Summary

✅ Installed Airflow & Dependencies  
✅ Created a MongoDB Atlas Account  
✅ Connected to the **makertdata** MongoDB Cluster  
✅ Configured API & Database Credentials  
✅ Started Airflow & Verified Execution  

Now your DAG will **fetch stock data daily at 5 PM** and store it in your **MongoDB Cluster!** 🚀


## Important Note:

- Ensure that the 'config.yml' has the correct API keys and MongoDB connection URI are correct.
- Ensure that filepaths are set up correctly in these files:
    
