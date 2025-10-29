import pandas as pd
import numpy as np
import pickle
import sys

# Load model and data
print("Loading model...")
with open('models/lightfm_model.pkl', 'rb') as f:
    model = pickle.load(f)

with open('models/dataset.pkl', 'rb') as f:
    dataset = pickle.load(f)

movies = pd.read_csv('data/processed/movies_clean.csv')
train = pd.read_csv('data/processed/train.csv')


def get_recommendations(user_idx, n_recommendations=10):
    """Get top N movie recommendations for a user"""

    # Get all movie indices
    n_movies = dataset.interactions_shape()[1]

    # Get movies already rated by user
    user_movies = set(train[train['user_idx'] == user_idx]['movie_idx'].values)

    # Predict scores for all movies
    scores = model.predict(user_ids=np.full(
        n_movies, user_idx), item_ids=np.arange(n_movies), num_threads=4)

    # Create list of (movie_idx, score) for unrated movies
    recommendations = []
    for movie_idx, score in enumerate(scores):
        if movie_idx not in user_movies:
            recommendations.append((movie_idx, score))

    # Sort by score and get top N
    recommendations.sort(key=lambda x: x[1], reverse=True)
    top_recs = recommendations[:n_recommendations]

    # Get movie details
    results = []
    for movie_idx, score in top_recs:
        movie_info = movies[movies['movie_idx'] == movie_idx].iloc[0]
        results.append({
            'movie_idx': movie_idx,
            'movieId': movie_info['movieId'],
            'title': movie_info['title'],
            'genres': movie_info['genres'],
            'score': float(score)
        })

    return results


# Test with a sample user
if len(sys.argv) > 1:
    user_idx = int(sys.argv[1])
else:
    user_idx = 100  # default test user

print(f"\nGenerating recommendations for user {user_idx}...")

# Show what user has rated
user_history = train[train['user_idx'] == user_idx].merge(
    movies, on='movie_idx'
)[['title', 'rating']].sort_values('rating', ascending=False).head(10)

print(f"\nUser's top rated movies:")
print(user_history.to_string(index=False))

# Get recommendations
recommendations = get_recommendations(user_idx, n_recommendations=10)

print(f"\nTop 10 recommendations:")
for i, rec in enumerate(recommendations, 1):
    print(f"{i}. {rec['title']} (score: {rec['score']:.3f})")
    print(f"   Genres: {rec['genres']}\n")
