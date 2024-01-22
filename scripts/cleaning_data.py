"""
    PySpark
"""

from pyspark.sql import SparkSession

# Create Spark Session
spark = SparkSession.builder.master("local[*]").getOrCreate()
dt = spark.read.csv('/content/merged_data.csv', header = True, inferSchema = True)

"""
    Data Profiling
"""
# Show columns name
dt
# Show first 20 rows
dt.show()
# Show data type each column
dt.dtypes
dt.printSchema()

# Show Statistics data
dt.describe().show()
dt.select("price").describe().show()

# Find missing value
dt.summary("count").show()
dt.where(dt.user_id.isNull()).show()

"""
    EDA Non-Graphical
"""

dt.where(dt.price >= 1).show()
dt.where(dt.country == 'Canada').show()
dt.where(dt.timestamp.startswith("2021-04")).count()

"""
    EDA Graphical
"""

import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd

# Download whole data to RAM, but Spark download only header.
dt_pd = dt.toPandas()
dt_pd.head()

# Boxplot and Histogram
sns.boxplot(x = dt_pd['book_id'])
sns.histplot(dt_pd['price'], bins=10)

# Scatterplot
sns.scatterplot(data=dt_pd, x="book_id", y="price")

"""
    Data cleansing via Spark
    -   Data type
"""

dt.printSchema()
dt.select("timestamp").show(10)

from pyspark.sql import functions as f
dt_clean = dt.withColumn("timestamp",
                        f.to_timestamp(dt.timestamp, 'yyyy-MM-dd HH:mm:ss')
dt.printSchema()

# Example 
# Count the transaction in first 15th in July
dt_clean.where( (f.dayofmonth(dt_clean.timestamp) <= 15) & ( f.month(dt_clean.timestamp) == 6 ) ).count()

"""
    Syntactical Anomalies
"""

# Show the data in column country
dt_clean.select("Country").distinct().sort("Country").show(58, False)
dt_clean.where(dt_clean['Country'] == 'Japane').show()
from pyspark.sql.functions import when

# Create new column Country_update and change Japane to Japan
dt_clean_country = dt_clean.withColumn("CountryUpdate",
                                       when(dt_clean['Country'] == 'Japane', 'Japan').otherwise(dt_clean['Country']))
# Delete Country column and rename CountryUpdate to Country
dt_clean = dt_clean_country.drop("Country").withColumnRenamed('CountryUpdate', 'Country')

"""
    Semantic Anomalies
"""

# user id have to 8 numbers
dt_clean.select("user_id").show(10)
# Count the correct user id by "Regular Expression"
dt_clean.where(dt_clean["user_id"].rlike("^[a-z0-9]{8}$")).count()

# Collect correct data
dt_correct_userid = dt_clean.where(dt_clean["user_id"].rlike("^[a-z0-9]{8}$"))
dt_incorrect_userid = dt_clean.subtract(dt_correct_userid)
dt_clean_userid = dt_clean.withColumn("user_id_update",
                                       when(dt_clean['user_id'] == 'ca86d17200', 'ca86d172').otherwise(dt_clean['user_id']))
dt_clean = dt_clean_userid.drop("user_id").withColumnRenamed('user_id_update', 'user_id')

"""
    Missing values
"""

# Find the missing value
from pyspark.sql.functions import col, sum
dt_clean.summary("count").show()
dt_clean.where( dt_clean.user_id.isNull() ).show()

# Convert the missing values to 00000000
dt_clean_user_id = dt_clean.withColumn("user_id_update",
                                       when(dt_clean['user_id'].isNull(), '00000000').otherwise(dt_clean['user_id']))
dt_clean = dt_clean_user_id.drop("user_id").withColumnRenamed('user_id_update', 'user_id')
dt_clean.show()

"""
    Outliers
"""
dt_clean_pd = dt_clean.toPandas()
sns.boxplot(x = dt_clean_pd['price'])
# Count the books price > 80$
dt_clean.where( dt_clean.price > 80 ).select("book_id").distinct().show()

"""
    Write to CSV
"""
dt_clean.write.csv('Cleaned_data.csv', header = True)


"""
    SparkSQL
"""

dt.createOrReplaceTempView("data")
dt_sql = spark.sql("SELECT * FROM data")
dt_sql.show()

dt_sql_country = spark.sql("""
SELECT distinct country
FROM data
ORDER BY country
""")