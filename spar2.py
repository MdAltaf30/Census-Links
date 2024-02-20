import os
import sys
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, concat, col
import json
from Levenshtein import distance

# Set environment variables
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Create Spark session
spark = SparkSession.builder.master("local[*]").appName("EntityResolution").getOrCreate()

# Read CSV files
df1 = spark.read.csv("C:/Users/altaf/Downloads/Truthorgi.csv", header=True, inferSchema=True)
df2 = spark.read.csv("C:/Users/altaf/Downloads/Easyorgi.csv", header=True, inferSchema=True)

# Add source column
df1 = df1.withColumn("TID", concat(lit("T"), col("TruthRowNum")))
df2 = df2.withColumn("EID", concat(lit("E"), col("TruthFileRowNum")))

# Select relevant columns for joining
df1_names = df1.select("TID", "First", "Last").withColumnRenamed("First", "T_FirstName").withColumnRenamed("Last", "T_LastName")
df2_names = df2.select("EID", "First Name", "Last Name").withColumnRenamed("First Name", "E_FirstName").withColumnRenamed("Last Name", "E_LastName")

# Combine dataframes
combined_df = df1.select("SSN", "TID").join(df2.select("SSN", "EID"), on="SSN", how="inner").sort("SSN")

# Join with names
combined_df = combined_df.join(df1_names, on="TID", how="inner").join(df2_names, on="EID", how="inner")

# Function to calculate similarity score between two IDs and their first names
def calculate_score(id1, id2, name1, name2, last_name1, last_name2):
    id1_no_prefix = id1[1:]  # Remove prefix
    id2_no_prefix = id2[1:]  # Remove prefix
    edit_distance_id = distance(id1_no_prefix, id2_no_prefix)
    similarity_score_id = 1 - (edit_distance_id / max(len(id1_no_prefix), len(id2_no_prefix)))
    
    edit_distance_name = distance(name1.lower(), name2.lower())  # Assuming names are case-insensitive
    similarity_score_name = 1 - (edit_distance_name / max(len(name1), len(name2)))
    
    edit_distance_last_name = distance(last_name1.lower(), last_name2.lower())  # Assuming last names are case-insensitive
    similarity_score_last_name = 1 - (edit_distance_last_name / max(len(last_name1), len(last_name2)))
    
    return similarity_score_id, similarity_score_name, similarity_score_last_name

# Generate ID pairs with similarity scores
id_pairs_with_scores = {}

# Collect all TIDs, EIDs, and first names for each SSN
ssn_ids_names_map = combined_df.rdd.map(lambda row: (row.SSN, (row.TID, row.EID, row.T_FirstName, row.E_FirstName, row.T_LastName, row.E_LastName))).collectAsMap()

# Iterate through each SSN and its corresponding TIDs, EIDs, and names
for ssn, (tid, eid, t_firstname, e_firstname, t_lastname, e_lastname) in ssn_ids_names_map.items():
    # Calculate similarity score between TID and EID pairs and their respective names
    score_id, score_name, score_last_name = calculate_score(tid, eid, t_firstname, e_firstname, t_lastname, e_lastname)
    
    # Store the scores in the dictionary
    if ssn not in id_pairs_with_scores:
        id_pairs_with_scores[ssn] = []
    id_pairs_with_scores[ssn].append((tid, eid, score_id, score_name, score_last_name))

# Save ID pairs with scores to a JSON file
output_file_path_with_scores = "id_pairs_with_scores.json"
with open(output_file_path_with_scores, "w") as json_file:
    json.dump(id_pairs_with_scores, json_file, indent=4)

print(f"ID pairs with scores saved to {output_file_path_with_scores}")

# Stop Spark session
spark.stop()