from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.dates import days_ago
import pandas as pd
import requests

# Create MySQL connection on Airflow (Credential)
MYSQL_CONNECTION = "mysql_default"
# Obtained conversion rate
CONVERSION_RATE_URL = "https://r2de2-workshop-vmftiryt6q-ts.a.run.app/usd_thb_conversion_rate"

# DataLake path
DATA_LAKE = "/home/airflow/gcs/data"

# Airflow's datalake path
BUCKET_NAME = "asia-east2-workshop4-2-cffd2a5a-bucket"

# Declare paths
mysql_output_path = "f{DATA_LAKE}/audible_data_merged.csv"
conversion_rate_output_path = "f{DATA_LAKE}/conversion_rate.csv"
final_output_path = "f{DATA_LAKE}/output.csv"

def get_data_from_mysql(transaction_path):
    mysqlserver = MySqlHook(MYSQL_CONNECTION)
    
    # Query from MySQL database
    audible_data = mysqlserver.get_pandas_df(sql="SELECT * FROM audible_data")
    audible_transaction = mysqlserver.get_pandas_df(sql="SELECT * FROM audible_transaction")

    # Merge data in book ID column
    df = audible_transaction.merge(audible_data, how="left", left_on="book_id", right_on="Book_ID")

    # Export CSV file to GCS
    df.to_csv(transaction_path, index=False)
    print(f"transaction data to {transaction_path}")

def get_conversion_rate(conversion_rate_path):
    r = requests.get(CONVERSION_RATE_URL)
    result_conversion_rate = r.json()
    df = pd.DataFrame(result_conversion_rate)
    df = df.reset_index().rename(columns={"index": "date"})
    df.to_csv(conversion_rate_path, index=False)
    print(f"conversion rate to {conversion_rate_path}")

def merge_data(transaction_path, conversion_rate_path, output_path):
    transaction = pd.read_csv(transaction_path)
    conversion_rate = pd.read_csv(conversion_rate_path)

    # Convert date column to to date format
    transaction['date'] = transaction['timestamp']
    transaction['date'] = pd.to_datetime(transaction['date']).dt.date
    conversion_rate['date'] = pd.to_datetime(conversion_rate['date']).dt.date

    # Merge 2 dataframes (transaction and conversion rate)
    final_df = transaction.merge(conversion_rate, how="left", left_on="date", right_on="date")
    
    # Convert Price column from string to float
    final_df["Price"] = final_df.apply(lambda x: x["Price"].replace("$",""), axis=1)
    final_df["Price"] = final_df["Price"].astype(float)

    # Create THBPrice column
    final_df["THBPrice"] = final_df["Price"] * final_df["conversion_rate"]
    final_df = final_df.drop(["date", "book_id"], axis=1)

    # Export to CSV
    final_df.to_csv(output_path, index=False)
    print(f"Merged data to {output_path}")

with DAG(
    "audiobook_pipeline",
    start_date=days_ago(1),
    schedule_interval="@once",
    tags=["audiobook"]
) as dag:

    t1 = PythonOperator(
        task_id="get_data_from_mysql",
        python_callable=get_data_from_mysql,
        op_kwargs={"transaction_path": mysql_output_path},
    )

    t2 = PythonOperator(
        task_id="get_conversion_rate",
        python_callable=get_conversion_rate,
        op_kwargs={"conversion_rate_path": conversion_rate_output_path},
    )

    t3 = PythonOperator(
        task_id="merge_data",
        python_callable=merge_data,
        op_kwargs={
            "transaction_path": mysql_output_path,
            "conversion_rate_path": conversion_rate_output_path, 
            "output_path": final_output_path
        },
    )

    # Load merged data into Data Warehouse Google Big Query (table workshop_5.audible_data)
    t4 = BashOperator(
        task_id='bq_load',
        bash_command=f'bq load \
            --source_format=CSV  --autodetect \
            workshop_5.audible_data \
            gs://{BUCKET_NAME}/dags/data/output.csv'
    )

    [t1, t2] >> t3 >> t4