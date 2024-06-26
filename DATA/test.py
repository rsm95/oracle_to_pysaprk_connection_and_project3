import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

#Jar paths
jdbc_jars = [
    r"C:\Users\rahul\Desktop\project1234\ojdbc6-11.jar",
    r"C:\Users\rahul\Desktop\project1234\postgresql-42.7.3.jar"
]

# Join the paths with commas
jdbc_jars_str = ",".join(jdbc_jars)



# Initialize Spark session

spark = SparkSession.builder \
    .appName("WriteToAzureBlobStorage") \
    .config("spark.jars", jdbc_jars_str) \
    .master("local[*]") \
    .getOrCreate()

# Oracle connection parameters
host = "localhost"
port = "1521"
schema = "XE"
URL = f"jdbc:oracle:thin:@{host}:{port}:{schema}"


# Loading data from Oracle into DataFrame
query = "SELECT * FROM testing"
df = spark.read.format("jdbc") \
    .option("url", URL) \
    .option("query", query) \
    .option("user", "sys as sysdba") \
    .option("password", "1995") \
    .option("driver", "oracle.jdbc.driver.OracleDriver") \
    .load()

df.printSchema()

print("Number of rows of given dataset are : - ",df.count())
df.show()

df2 = df.select(df.DATA_VALUE).count()
print("Number of Before dropping nulls of column DATA_VALUE : - ",df2)

df3 = df.na.drop(subset = ['DATA_VALUE'])
df4 = df3.select(df.DATA_VALUE).count()
print("Number of After dropping nulls of column DATA_VALUE : - ",df4)

df.show()

df5 = df.fillna({'SUPPRESSED':'#########'})
df5.show()