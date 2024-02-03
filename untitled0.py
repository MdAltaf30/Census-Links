# -*- coding: utf-8 -*-
"""
Created on Wed Jan 31 03:58:51 2024

@author: altaf
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, col, sha2
print("altaf")
# Create a Spark session
spark = SparkSession.builder.appName("UnionAll").getOrCreate()
print("altaf")
# Assuming you have a CSV file named 'your_file.csv'
file_path = 'C:/Users/altaf/Downloads/Easy500.csv'

print("altaf")
# Read the CSV file into a DataFrame
df = spark.read.csv(file_path, header=True)

# Define the columns you want to include in the hash keydsd
column1 = "First Name"  # Replace with the actual column name
column2 = "Last Name"  # Replace with the actual column namexsxs
# Extract the first 2 characters from the first column
first_2_chars = col(column1).substr(1, 2)

# Extract the last 2 characters from the second column
last_2_chars = col(column2).substr(-2, 2)

# Concatenate the extracted characters into a single column
concatenated_column = concat(first_2_chars, last_2_chars)
df_with_concatenated_column = df.withColumn("concatenated_column", concatenated_column)
df_with_concatenated_column.select("concatenated_column").show(truncate=False)
# Apply the SHA-2 hash function to the concatenated column
hash_key_column = sha2(concatenated_column, 256).alias("hash_key")
# Add the hash key column to the DataFrame
df_with_hash = df.withColumn("hash_key", hash_key_column)

# Show the resulting DataFrame with the hash key

df_with_hash.show(truncate=False)