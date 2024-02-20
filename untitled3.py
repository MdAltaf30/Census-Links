# -*- coding: utf-8 -*-
"""
Created on Mon Feb 19 03:17:52 2024

@author: altaf
"""

import json

# Read the ID pairs with maximum scores from file4.json
with open("file4.json", "r") as json_file:
    max_score_pairs = json.load(json_file)

# Remove the scores from the pairs
id_pairs = [(tid, eid) for tid, (eid, _) in max_score_pairs]

# Save the ID pairs without scores to file5.json
output_file_path = "file5.json"
with open(output_file_path, "w") as json_file:
    json.dump(id_pairs, json_file, indent=4)

print(f"ID pairs without scores saved to {output_file_path}")
