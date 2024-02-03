# -*- coding: utf-8 -*-
"""
Created on Mon Jan 29 04:40:51 2024

@author: altaf
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, concat_ws, col
 
# Create a Spark session
spark = SparkSession.builder.appName("UnionAllExample").getOrCreate()
 
# Read the CSV files into DataFrames
df1 = spark.read.csv('2020FileClusters.csv', header=True)
df2 = spark.read.csv('2030FileClusters.csv', header=True)
df3 = spark.read.csv('data.csv', header=True)
 
df1 = df1.withColumn("Record_ID", col("ID"))
df2 = df2.withColumn("Record_ID", col("ID"))
# Add a column to distinguish the source of data
df1 = df1.withColumn("Record_ID", concat_ws("_", col("Record_ID"), lit("Truth")))
df2 = df2.withColumn("Record_ID", concat_ws("_", col("Record_ID"), lit("Easy")))
 
# Perform UNION ALL
result_df = df1.unionAll(df2)
 
# Add a new column "id_copy" which is a duplicate of the existing "ID" column
 
# Join operation using the modified "ID" column
last_df = result_df.join(df3, on='ID', how='inner')
# Order the result by 'ssn'
last_df_ordered = last_df.orderBy("ssn")
last_df_ordered=last_df_ordered.drop("ID")
ssn_cluster_dict = last_df.select("ssn", "Cluster").rdd.collectAsMap()
 
# Show the ordered result
last_df_ordered.show()
with open('ssn_cluster_dict.csv', 'w') as file:
    for ssn, cluster in ssn_cluster_dict.items():
        file.write(f"{ssn},{cluster}\n")
 #

print(ssn_cluster_dict)
 
# Stop the Spark session
spark.stop()