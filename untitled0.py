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
df1 = df1.withColumn("source", concat(lit("T"), col("TruthRowNum")))
df2 = df2.withColumn("source", concat(lit("E"), col("TruthFileRowNum")))
 
# Combine dataframes
combined_df = df1.select("SSN", "source").union(df2.select("SSN", "source")).sort("SSN")
print("Executed")
def map_function(row):
    ssn = row.SSN
    ids = [row.source]
    return ssn, ids

# Apply map function and reduceByKey
result_rdd = combined_df.rdd.map(map_function).reduceByKey(lambda x, y: x + y)
result_filtered = result_rdd.filter(lambda x: len(x[1])>2)
result_filtereds = dict(sorted(result_filtered.collectAsMap().items()))
json_data = json.dumps(result_filtereds, indent=2)
 
# Specify the output file path
output_path = "file1.json"
 
# Write the JSON data to a file
with open(output_path, "w") as json_file:
     json_file.write(json_data)

print(f"ID pairs with scores saved to {output_path}")

id_pairs = []

# Iterate over the items in the dictionary
for ssn, ids in result_filtereds.items():
    # Extract IDs from truth and easy files
    truth_ids = [id_ for id_ in ids if id_.startswith("T")]
    easy_ids = [id_ for id_ in ids if id_.startswith("E")]

    # Generate pairs from truth to easy
    for truth_id in truth_ids:
        for easy_id in easy_ids:
            id_pairs.append((truth_id, easy_id))

output_file_path = "file2.json"
id_pairs_set = set(id_pairs)
with open(output_file_path, "w") as json_file:
    json.dump(id_pairs, json_file, indent=4)   

print(f"ID pairs saved to {output_file_path}")

# Function to calculate similarity score between TID and EIDs based on first and last name
def calculate_score(tid, eids, df1, df2):
    tid_row = df1.filter(df1["source"] == tid).select("First", "Last").collect()[0]
    tid_first_name = tid_row["First"]
    tid_last_name = tid_row["Last"]
    eid_scores = []
    for eid in eids:
        eid_row = df2.filter(df2["source"] == eid).select("First Name", "Last Name").collect()[0]
        eid_first_name = eid_row["First Name"]
        eid_last_name = eid_row["Last Name"]
        # Calculate similarity score for first name
        first_name_distance = distance(tid_first_name.lower(), eid_first_name.lower())
        first_name_score = 1 - (first_name_distance / max(len(tid_first_name), len(eid_first_name)))
        # Calculate similarity score for last name
        last_name_distance = distance(tid_last_name.lower(), eid_last_name.lower())
        last_name_score = 1 - (last_name_distance / max(len(tid_last_name), len(eid_last_name)))
        # Average the scores for first and last name
        avg_score = (first_name_score + last_name_score) / 2
        eid_scores.append((eid, avg_score))
    return eid_scores
print("Executed")
# Generate ID pairs with scores
id_pairs_with_scores = []
 
# Iterate through each SSN and its corresponding TIDs, EIDs
for ssn, ids in result_filtereds.items():
    tid = ids[0]  # TID is the first element
    eids = ids[1:]  # Rest are EIDs
    scores = calculate_score(tid, eids, df1, df2)
    id_pairs_with_scores.extend([(tid, eid, score) for eid, score in scores])
print("Executed")
# Save ID pairs with scores to a JSON file
output_file_path_with_scores = "file3.json"
with open(output_file_path_with_scores, "w") as json_file:
    json.dump(id_pairs_with_scores, json_file, indent=4)
 
print(f"ID pairs with scores saved to {output_file_path_with_scores}")
 
 
# Group the ID pairs with scores by TID
tid_groups = {}
for tid, eid, score in id_pairs_with_scores:
    if tid not in tid_groups:
        tid_groups[tid] = []
    tid_groups[tid].append((eid, score))
 
# Select the pair with the maximum score for each TID
max_score_pairs = [(tid, max(scores, key=lambda x: x[1])) for tid, scores in tid_groups.items()]
 
# Save ID pairs with maximum scores to a JSON file
output_file_path_max_scores = "file4.json"
with open(output_file_path_max_scores, "w") as json_file:
    json.dump(max_score_pairs, json_file, indent=4)
 
print(f"ID pairs with maximum scores saved to {output_file_path_max_scores}")
 
# Stop Spark session
spark.stop()