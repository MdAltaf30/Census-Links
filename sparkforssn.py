"""This program takes two files 
Map the records with the same ssn and copy to output.json
then make the possible pairs from file A to B and copy to idpairs.json
and remove the duplicates and copy the unique pairs to id_pairs_unique.json"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, concat, col
import os
import sys
import findspark
findspark.init()
 
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark = SparkSession.builder.master("local[*]").appName("EntityResolution").getOrCreate()
 
df1 = spark.read.csv("C:/Users/altaf/Downloads/Truthorgi.csv", header=True, inferSchema=True)
df2 = spark.read.csv("C:/Users/altaf/Downloads/Easyorgi.csv", header=True, inferSchema=True)
 
 
df1 = df1.withColumn("source", concat(lit("T"),col("TruthRowNum")))
df2 = df2.withColumn("source", concat( lit("E"),col("TruthFileRowNum")))
 
 
 
combined_df = df1.select("SSN", "source").union(df2.select("SSN", "source"))
sorted_df = combined_df.sort("SSN")
 
def map_function(row):
     ssn = row.SSN
     ids = [row.source]
     return ssn, ids 
 
# Apply map function and reduceByKey
result_rdd = combined_df.rdd.map(map_function).reduceByKey(lambda x, y: x + y)
result_filtered = result_rdd.filter(lambda x: len(x[1]) > 2)
import json
result_filtereds = dict(sorted(result_filtered.collectAsMap().items()))

json_data = json.dumps(result_filtereds, indent=2)
output_path = "output.json"
with open(output_path, "w") as json_file:
     json_file.write(json_data)
42480564
print(f"Output saved to {output_path}")


'''code for generating pairs starts'''
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
id_pairs.sort()
id_pairs_count = len(id_pairs)
print("Count of ID pairs:", id_pairs_count)

output_file_path = "id_pairs.json"
id_pairs_set = set(id_pairs)
with open(output_file_path, "w") as json_file:
    json.dump(id_pairs, json_file, indent=4)   

print(f"ID pairs saved to {output_file_path}")
'''code for generating pairs ends'''





import random

# Let's generate random scores for each pair of IDs
scores = [random.randint(1, 100) for _ in range(len(id_pairs))]
id_pairs_with_scores = {}

for i, pair in enumerate(id_pairs):
    t_id, e_id = pair
    score = scores[i]  # assuming scores are associated with pairs in order
    if t_id not in id_pairs_with_scores:
        id_pairs_with_scores[t_id] = []
    id_pairs_with_scores[t_id].append((e_id, score))

# Now, id_pairs_with_scores contains the desired dictionary.

# If you want to write this dictionary to a JSON file, you can do it like this:
output_file_path_with_scores = "id_pairs_with_scores.json"
with open(output_file_path_with_scores, "w") as json_file:
    json.dump(id_pairs_with_scores, json_file, indent=4)

print(f"ID pairs with scores saved to {output_file_path_with_scores}")

# If you want to print this dictionary, you can do it like this:

    




max_scores_with_ids = {}

# Iterate through the id_pairs_with_scores dictionary
for t_id, pairs in id_pairs_with_scores.items():
    max_score = max(pair[1] for pair in pairs)  # Get the maximum score for the current T ID
    max_pairs = [(e_id, score) for e_id, score in pairs if score == max_score]  # Get pairs with max score
    max_scores_with_ids[t_id] = max_pairs
    
# Write the new dictionary to a JSON file
output_file_path_max_scores = "max_scores_with_ids.json"
with open(output_file_path_max_scores, "w") as json_file:
    json.dump(max_scores_with_ids, json_file, indent=4)

print(f"Max scores with IDs saved to {output_file_path_max_scores}")













'''code for generating unique pairs starts'''
unique_id_pairs_set = set()

for pair in id_pairs:
    if tuple(pair) not in unique_id_pairs_set:
        unique_id_pairs_set.add(tuple(pair))

unique_id_pairs = list(unique_id_pairs_set)
unique_id_pairs_count = len(unique_id_pairs)

print("Count of unique ID pairs:", unique_id_pairs_count)

output_file_p = "id_pairs_unique.json"
with open(output_file_p, "w") as json_file:
    json.dump(unique_id_pairs, json_file, indent=2)

print(f"ID unique pairs saved to {output_file_p}")
'''code for generating unique pairs ends'''




# Print 5 SSNs with more than one ID
for ssn, ids in result_filtered.take(5):
     print(f"SSN: {ssn}, IDs: {ids}")
spark.stop()