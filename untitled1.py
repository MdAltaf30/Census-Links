import os
import sys
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, concat, col
import json
from Levenshtein import distance

findspark.init()

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.master("local[*]").appName("EntityResolution").getOrCreate()

df1 = spark.read.csv("C:/Users/altaf/Downloads/Truthorgi.csv", header=True, inferSchema=True)
df2 = spark.read.csv("C:/Users/altaf/Downloads/Easyorgi.csv", header=True, inferSchema=True)


df1 = df1.withColumn("source", concat(lit("T"), col("TruthRowNum")))
df2 = df2.withColumn("source", concat(lit("E"), col("TruthFileRowNum")))

combined_df = df1.select("SSN", "source").union(df2.select("SSN", "source"))

def map_function(row):
    ssn = row.SSN
    ids = [row.source]
    return ssn, ids

# Apply map function and reduceByKey
result_rdd = combined_df.rdd.map(map_function).reduceByKey(lambda x, y: x + y)

import json

# Convert RDD to dictionary
result_filtereds = result_rdd.collectAsMap()

# Convert dictionary to JSON format
json_data = json.dumps(result_filtereds, indent=2)

# Specify the output file path
output_path = "out.json"

# Write the JSON data to a file
with open(output_path, "w") as json_file:
    json_file.write(json_data)

print(f"Output saved to {output_path}")

# Print 5 SSNs with their associated IDs
for ssn, ids in result_rdd.take(5):
    print(f"SSN: {ssn}, IDs: {ids}")


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

output_file_path = "idpairs.json"
id_pairs_set = set(id_pairs)
with open(output_file_path, "w") as json_file:
    json.dump(id_pairs, json_file, indent=4)   
   
print(f"ID pairs saved to {output_file_path}")




def calculate_name_score(name1, name2):
    distance_score = distance(name1.lower(), name2.lower())
    max_length = max(len(name1), len(name2))
    similarity_score = 1 - (distance_score / max_length)
    return similarity_score

# Function to calculate average similarity score for a pair of IDs
def calculate_pair_score(tid, eid, df1, df2):
    tid_row = df1.filter(df1["source"] == tid).select("First", "Last").collect()[0]
    eid_row = df2.filter(df2["source"] == eid).select("First Name", "Last Name").collect()[0]
    
    first_name_score = calculate_name_score(tid_row["First"], eid_row["First Name"])
    last_name_score = calculate_name_score(tid_row["Last"], eid_row["Last Name"])
    
    # Calculate average score
    avg_score = (first_name_score + last_name_score) / 2
    return avg_score

# Calculate scores for each pair and store in a list
pair_scores = []
for tid, eid in id_pairs:
    score = calculate_pair_score(tid, eid, df1, df2)
    pair_scores.append((tid, eid, score))

# Specify the output file path for the scores
scores_output_file = "idpair_scores.json"

# Write the scores to a JSON file
with open(scores_output_file, "w") as json_file:
    json.dump(pair_scores, json_file, indent=4)

print(f"Scores for ID pairs saved to {scores_output_file}")



 

spark.stop()
