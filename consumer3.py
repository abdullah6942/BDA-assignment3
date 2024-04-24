from collections import defaultdict
import json

# Constants
MIN_SUPPORT = 10  # Adjust the minimum support threshold as needed

# Initialize frequent itemset dictionary
frequent_itemsets = defaultdict(int)

# Function to generate candidate itemsets
def generate_candidates(transaction):
    # Generate candidate itemsets based on the transaction
    candidate_itemsets = []
    for item in transaction:
        candidate_itemsets.append((item,))
    return candidate_itemsets

# Function to initialize single-item frequent itemsets
def initialize_single_itemsets(transactions):
    global frequent_itemsets
    for transaction in transactions:
        for item in transaction:
            frequent_itemsets[(item,)] += 1

# Function to prune infrequent itemsets
def prune_infrequent_itemsets():
    global frequent_itemsets
    # Prune infrequent itemsets from the dictionary
    frequent_itemsets = {itemset: support for itemset, support in frequent_itemsets.items() if support >= MIN_SUPPORT}

# Simulate streaming data
def stream_data():
    # Simulate streaming data
    for transaction in stream_data_generator():
        # Generate candidates and update frequent itemsets with new transaction
        candidates = generate_candidates(transaction)
        for candidate in candidates:
            frequent_itemsets[candidate] += 1
        # Prune infrequent itemsets
        prune_infrequent_itemsets()
        # Output frequent itemsets
        print_frequent_itemsets()

# Function to simulate streaming data generator (replace with your actual data source)
def stream_data_generator():
    # Simulated data source
    for i in range(10000):
        yield ['item1', 'item2', 'item3']  # Example transaction format

# Function to print frequent itemsets
def print_frequent_itemsets():
    # Output frequent itemsets that meet the minimum support threshold
    for itemset, support in frequent_itemsets.items():
        if support >= MIN_SUPPORT:
            print(f"Frequent Itemset: {itemset}, Support: {support}")

# Main function
def main():
    # Initialize single-item frequent itemsets
    initialize_single_itemsets(stream_data_generator())
    # Process streaming data
    stream_data()

if __name__ == "__main__":
    main()

