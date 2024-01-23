# Audiobooks Project

This project involves gathering and processing data for a Data Analyst at an Audiobook company. We start with data from the company's database (MySQL), collected from the company website and through an API. Then, we set up a pipeline to prepare and store this data in a data warehouse. We will also use this data to make a dashboard that shows the best-selling products. This will help in choosing which products to promote and planning promotions to boost audiobook sales.

## Files description
- audiobook_dags.py: Data pipeline by using Apache Airflow.
- cleansing_data.py: Sample python script to clean the data via PySpark.

## Procedure of this course
1. Learn how to extract and transform data via colab notebook
    - Query the data in database(MySQL), It has 2 tables: audible_data and audible_transaction.
    - Query the conversion rate data via REST API.
    - Transform the data.
    - Perform the data profiling and data cleansing via PySpark (Apache Spark)
1. Create the data pipeline (Google Cloud Composer) and install python packages in cluster(pymysql, requests, pandas) for running Apache Airflow.
2. **[ Credential ]** Set MySQL connection on Apache Aiflow web server (Admin ==> Connection ==> mysql_default)
3. Upload pipeline script : *audiobook.py* to **dags** folder via Cloud shell(gsutill), example:

    `gsutil cp scripts/audiobook.py gs://asia-east2-workshop4-2-cffd2a5a-bucket/dags`

4. The data pipeline is shown below:
    ![DAG](https://github.com/Tarzoozoo/audiobook_r2de/blob/main/pictures/dag.png)  

7. Create a view in Google BigQuery, then create the sales dashboard in Looker Studio

    [Sales Dashboard](https://lookerstudio.google.com/u/0/reporting/8e7759b0-3a21-4465-9eb0-3e3cd059d09b/page/XLyeD)

## Tools
- MySQL
- Google Cloud Platform
    - Google Cloud Storage
    - Google Cloud BigQuery
    - Google Cloud Composer
    - Looker Studio
- Apache Airflow
- Apache Spark
- Python
- SQL