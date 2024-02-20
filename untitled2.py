import json

def custom_sort(pair):
    """
    Custom sorting function to sort ID pairs ignoring the 'T' or 'E' prefix.
    """
    # Extract the numeric part of the IDs and convert them to integers
    id1 = int(pair[0][1:])  # Extract from index 1 to the end
    id2 = int(pair[1][1:])  # Extract from index 1 to the end
    
    # Compare the numeric parts and return the result
    if id1 == id2:
        return 0
    elif id1 < id2:
        return -1
    else:
        return 1

# Read the id_pairs from the JSON file
with open('id_pairs.json', 'r') as file:
    id_pairs = json.load(file)

# Sort the ID pairs using the custom sorting function
sorted_id_pairs = sorted(id_pairs, key=custom_sort)

# Print the sorted ID pairs


# Save the sorted ID pairs to a JSON file
sorted_output_file_path = "sorted_id_pairs.json"
with open(sorted_output_file_path, "w") as json_file:
    json.dump(sorted_id_pairs, json_file, indent=4)

print(f"Sorted ID pairs saved to {sorted_output_file_path}")

