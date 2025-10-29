import pandas as pd
import numpy as np
from lightfm.data import Dataset
import pickle
from tqdm import tqdm

print("Loading data...")
train = pd.read_csv('data/processed/train.csv')
test = pd.read_csv('data/processed/test.csv')

print(f"Initial train: {len(train):,} ratings")
print(f"Initial test: {len(test):,} ratings")

# Filter test set to only include users and movies seen in training
train_users = set(train['user_idx'].unique())
train_movies = set(train['movie_idx'].unique())

print("\nFiltering test set to known users/movies...")
test = test[
    test['user_idx'].isin(train_users) &
    test['movie_idx'].isin(train_movies)
]

print(f"\nAfter filtering:")
print(f"Train: {len(train):,} ratings")
print(f"Test: {len(test):,} ratings")
print(f"Users: {train['user_idx'].nunique():,}")
print(f"Movies: {train['movie_idx'].nunique():,}")

# Build LightFM dataset
print("\nInitializing dataset...")
dataset = Dataset()
dataset.fit(
    users=train['user_idx'].unique(),
    items=train['movie_idx'].unique()
)

print("\nBuilding train interaction matrix...")
print("Converting to triplets...")
train_triplets = list(zip(
    train['user_idx'].values,
    train['movie_idx'].values,
    train['rating'].values
))
print(f"Train triplets: {len(train_triplets):,}")

print("Building sparse matrix (this takes time)...")
(train_interactions, train_weights) = dataset.build_interactions(
    tqdm(train_triplets, desc="Train interactions")
)

print(f"Train matrix shape: {train_interactions.shape}")
print(f"Train non-zero entries: {train_interactions.nnz:,}")

print("\nBuilding test interaction matrix...")
print("Converting to triplets...")
test_triplets = list(zip(
    test['user_idx'].values,
    test['movie_idx'].values,
    test['rating'].values
))
print(f"Test triplets: {len(test_triplets):,}")

print("Building sparse matrix...")
(test_interactions, test_weights) = dataset.build_interactions(
    tqdm(test_triplets, desc="Test interactions")
)

print(f"Test matrix shape: {test_interactions.shape}")
print(f"Test non-zero entries: {test_interactions.nnz:,}")

# Save everything
print("\nSaving interactions and dataset...")
with open('models/dataset.pkl', 'wb') as f:
    pickle.dump(dataset, f)

with open('models/train_interactions.pkl', 'wb') as f:
    pickle.dump((train_interactions, train_weights), f)

with open('models/test_interactions.pkl', 'wb') as f:
    pickle.dump((test_interactions, test_weights), f)

print("\nSaved:")
print("  - models/dataset.pkl")
print("  - models/train_interactions.pkl")
print("  - models/test_interactions.pkl")
print("\nReady for training!")
