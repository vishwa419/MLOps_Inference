import pandas as pd
import numpy as np

print("Loading processed ratings...")
ratings = pd.read_csv('data/processed/ratings_clean.csv')

print(f"Total ratings: {len(ratings)}")

# Sort by timestamp
ratings = ratings.sort_values('timestamp')

# Use last 20% as test set (chronological split)
split_idx = int(len(ratings) * 0.8)

train = ratings.iloc[:split_idx]
test = ratings.iloc[split_idx:]

rated = len(train)/len(ratings)

print(f"\nTrain set: {len(train)} ratings ({rated*100:.1f}%)")
rated = len(test)/len(ratings)
print(f"Test set: {len(test)} ratings ({rated*100:.1f}%)")

print(f"\nTrain date range: {pd.to_datetime(train['timestamp'], unit='s').min()} to {pd.to_datetime(train['timestamp'], unit='s').max()}")
print(f"Test date range: {pd.to_datetime(test['timestamp'], unit='s').min()} to {pd.to_datetime(test['timestamp'], unit='s').max()}")

# Save splits
train.to_csv('data/processed/train.csv', index=False)
test.to_csv('data/processed/test.csv', index=False)

print("\nFiles saved:")
print("  - data/processed/train.csv")
print("  - data/processed/test.csv")
