
# Home Sales Data Analysis

This notebook, `Home_Sales.ipynb`, demonstrates how to process, analyze, and manipulate home sales data using Apache Spark in Google Colab. It covers data loading, caching, partitioning, and creating temporary views with Parquet formatted data.

## Prerequisites

Before running the notebook, ensure you have the following:
- A Google Colab environment.
- Access to the internet to download required packages and data.

## Steps

### 1. Install and Set Up Spark and Java
Install Apache Spark and Java, set environment variables, and initialize Spark.

\`\`\`python
!apt-get update
!apt-get install openjdk-11-jdk-headless -qq > /dev/null
!wget -q http://www.apache.org/dist/spark/spark-3.4.3/spark-3.4.3-bin-hadoop3.tgz
!tar xf spark-3.4.3-bin-hadoop3.tgz
!pip install -q findspark

import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-3.4.3-bin-hadoop3"

import findspark
findspark.init()
\`\`\`

### 2. Import Libraries and Create a SparkSession
Import necessary libraries and create a Spark session.

\`\`\`python
from pyspark.sql import SparkSession
from pyspark import SparkFiles

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()
\`\`\`

### 3. Load Data into a DataFrame
Load the home sales data from a provided URL into a Spark DataFrame.

\`\`\`python
url = "https://2u-data-curriculum-team.s3.amazonaws.com/dataviz-classroom/v1.2/22-big-data/home_sales_revised.csv"
spark.sparkContext.addFile(url)
df = spark.read.csv(SparkFiles.get("home_sales_revised.csv"), header=True, inferSchema=True)

df.createOrReplaceTempView("home_sales")
\`\`\`

### 4. Cache the Temporary Table
Cache the `home_sales` temporary table to speed up subsequent queries.

\`\`\`python
spark.catalog.cacheTable("home_sales")
print(spark.catalog.isCached("home_sales"))
\`\`\`

### 5. Partition and Save Data as Parquet
Partition the DataFrame by the `date_built` field and save it in Parquet format to Google Drive.

\`\`\`python
from google.colab import drive
drive.mount('/content/drive')

output_path = "/content/drive/MyDrive/Home_Sales_parquet"
df.write.partitionBy("date_built").parquet(output_path)
print(f"Data has been successfully written to {output_path} partitioned by 'date_built'.")
\`\`\`

### 6. Read the Parquet Formatted Data
Read the Parquet formatted data back into a DataFrame.

\`\`\`python
parquet_df = spark.read.parquet(output_path)
parquet_df.printSchema()
parquet_df.show()
\`\`\`

### 7. Create a Temporary Table for Parquet Data
Create a temporary table for the Parquet data.

\`\`\`python
parquet_df.createOrReplaceTempView("parquet_home_sales")
spark.sql("SELECT * FROM parquet_home_sales LIMIT 10").show()
\`\`\`

### 8. Uncache the Temporary Table
Uncache the `home_sales` temporary table and verify it is no longer cached.

\`\`\`python
spark.catalog.uncacheTable("home_sales")
is_cached = spark.catalog.isCached("home_sales")
print(f"Is 'home_sales' table cached? {is_cached}")
\`\`\`

## Usage

Run the notebook cells sequentially to perform the data analysis tasks. Adjust the paths and parameters as needed for your specific use case.

## Conclusion

This notebook provides a comprehensive guide to using Apache Spark in Google Colab for processing and analyzing home sales data. It demonstrates data loading, caching, partitioning, and creating temporary views with Parquet formatted data.
