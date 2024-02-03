from ngram import NGram
from itertools import combinations
def preprocess_data(data):
    processed_data = []
    for record in data:
        # Preprocess the attributes (lowercase, remove special characters, etc.)
        name = record.get('Name', '').lower()
        address = record.get('Address', '').lower()
        ssn = record.get('SSN', '').replace('-', '')

        # Create a tuple or dictionary with the preprocessed attributes
        processed_record = {
            'Name': name,
            'Address': address,
            'SSN': ssn
        }
        processed_data.append(processed_record)

    return processed_data


def block_records(data, attribute, threshold):
    blocks = {}
    for i, record in enumerate(data):
        attribute_value = record[attribute]
        if attribute_value not in blocks:
            blocks[attribute_value] = []
        blocks[attribute_value].append(i)

    # Perform n-gram blocking within each block
    for block in blocks.values():
        for i, j in combinations(block, 2):
            attribute_value_i = data[i][attribute]
            attribute_value_j = data[j][attribute]
            similarity = NGram.compare(attribute_value_i, attribute_value_j)
            if similarity >= threshold:
                print(f"Potential match: Record {i} - Record {j}")

# Example usage
data = [
    {'Name': 'John Doe', 'Address': '123 Main St', 'SSN': '123-45-6789'},
    {'Name': 'Jane Smith', 'Address': '456 Elm St', 'SSN': '987-65-4321'},
    {'Name': 'John Smith', 'Address': '789 Oak St', 'SSN': '456-78-9012'},
    {'Name': 'Jonathan Doe', 'Address': '321 Maple St', 'SSN': '111-22-3333'},
]

# Preprocess the data
processed_data = preprocess_data(data)

# Set the blocking attribute and threshold
blocking_attribute = 'Name'
similarity_threshold = 0.1

# Perform blocking
print(block_records(processed_data, blocking_attribute, similarity_threshold))
