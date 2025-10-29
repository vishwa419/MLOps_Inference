import pandas as pd
import numpy as np
from datetime import datetime

print("Loading data...")
ratings = pd.read_csv('data/raw/ratings.csv')
movies = pd.read_csv('data/raw/movies.csv')

print(f"Initial ratings: {len(ratings)}")
print(f"Initial movies: {len(movies)}")

# Keep only movies that have ratings
rated_movie_ids = ratings['movieId'].unique()
movies_filtered = movies[movies['movieId'].isin(rated_movie_ids)]

print("\nAfter filtering movies with no ratings:")
print(f"Movies: {len(movies_filtered)}")

# Filter users and movies with minimum interactions
# Keep users with at least 5 ratings
user_counts = ratings['userId'].value_counts()
active_users = user_counts[user_counts >= 5].index
ratings_filtered = ratings[ratings['userId'].isin(active_users)]

print(f"\nAfter filtering users with <5 ratings:")
print(f"Ratings: {len(ratings_filtered)}")
print(f"Users: {ratings_filtered['userId'].nunique()}")

# Keep movies with at least 5 ratings
movie_counts = ratings_filtered['movieId'].value_counts()
popular_movies = movie_counts[movie_counts >= 5].index
ratings_filtered = ratings_filtered[ratings_filtered['movieId'].isin(
    popular_movies)]

print(f"\nAfter filtering movies with <5 ratings:")
print(f"Ratings: {len(ratings_filtered)}")
print(f"Movies: {ratings_filtered['movieId'].nunique()}")

# Re-index user and movie IDs to be continuous (required for LightFM)
print("\nRe-indexing user and movie IDs...")
user_id_map = {old_id: new_id for new_id,
               old_id in enumerate(ratings_filtered['userId'].unique())}
movie_id_map = {old_id: new_id for new_id,
                old_id in enumerate(ratings_filtered['movieId'].unique())}

ratings_filtered['user_idx'] = ratings_filtered['userId'].map(user_id_map)
ratings_filtered['movie_idx'] = ratings_filtered['movieId'].map(movie_id_map)

# Save mappings for later use
pd.DataFrame(list(user_id_map.items()), columns=['original_userId', 'user_idx']).to_csv(
    'data/processed/user_id_map.csv', index=False
)
pd.DataFrame(list(movie_id_map.items()), columns=['original_movieId', 'movie_idx']).to_csv(
    'data/processed/movie_id_map.csv', index=False
)

# Filter movies metadata
movies_filtered = movies_filtered[movies_filtered['movieId'].isin(
    ratings_filtered['movieId'].unique())]
movies_filtered['movie_idx'] = movies_filtered['movieId'].map(movie_id_map)

# Save processed data
print("\nSaving processed data...")
ratings_filtered.to_csv('data/processed/ratings_clean.csv', index=False)
movies_filtered.to_csv('data/processed/movies_clean.csv', index=False)

print("\n=== SUMMARY ===")
print(f"Final ratings: {len(ratings_filtered)}")
print(f"Final users: {ratings_filtered['user_idx'].nunique()}")
print(f"Final movies: {ratings_filtered['movie_idx'].nunique()}")
n_users = ratings_filtered['user_idx'].nunique()
n_movies = ratings_filtered['movie_idx'].nunique()
sparsity = 1 - len(ratings_filtered) / (n_users * n_movies)
print(f"Final sparsity: {sparsity:.4f}")
print(f"\nFiles saved:")
print(f"  - data/processed/ratings_clean.csv")
print(f"  - data/processed/movies_clean.csv")
print(f"  - data/processed/user_id_map.csv")
print(f"  - data/processed/movie_id_map.csv")
