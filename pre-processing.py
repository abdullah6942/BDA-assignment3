import pandas as pd
import json
import re

# Define a function to remove HTML tags from text
def remove_html_tags(text):
    if isinstance(text, str):
        clean = re.compile('<.*?>')
        return re.sub(clean, '', text)
    elif isinstance(text, list):
        return [remove_html_tags(item) for item in text]
    else:
        return text

# Processed chunk count
# Processed chunk count
processed_chunks = 0

# Load JSON file in chunks
chunk_size = 10000  # Adjust chunk size as needed
with open('Sampled_Amazon_eta.json', 'r') as file:
    for _ in range(processed_chunks * chunk_size):  # Skip rows if necessary
        next(file)
    
    for chunk in pd.read_json(file, lines=True, chunksize=chunk_size):
        # Remove unwanted columns
        unwanted_columns = ['imageURL', 'imageURL']
        chunk = chunk.drop(columns=unwanted_columns, errors='ignore')
        
        # Remove rows with null values
        chunk = chunk.dropna()

        # Remove HTML code from all fields
        for col in chunk.columns:
            chunk[col] = chunk[col].apply(remove_html_tags)

        # Remove rows containing "https"
        chunk = chunk[~chunk.apply(lambda row: row.astype(str).str.contains('https').any(), axis=1)]

        # Update processed chunk count
        processed_chunks += 1

        # Perform your data processing here
        #print(chunk.head())  # For demonstration purposes only, remove this line in actual processing

        # Write cleaned chunk to file
        with open('cleaned.json', 'a') as cleaned_file:
            chunk.to_json(cleaned_file, orient='records', lines=True)


################

chunk_size = 1000  # Adjust chunk size as needed
for chunk in pd.read_json('cleaned.json', lines=True, chunksize=chunk_size):
    # Display the head of the chunk
    print(chunk.head())

##############

import pandas as pd

# Load previously cleaned data from "cleaned.json"
cleaned_data = pd.read_json('cleaned.json', lines=True)

# Drop columns with null values
cleaned_data = cleaned_data.dropna(axis=1)

# Drop rows with null values
cleaned_data = cleaned_data.dropna()

# Save further cleaned data to "cleaned2.json"
cleaned_data.to_json('cleaned2.json', orient='records', lines=True)