import bentoml
import pandas as pd
import numpy as np
from typing import List, Dict, Any
import pickle
import time
import requests
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST

# Prometheus metrics
REQUEST_COUNT = Counter('recommender_requests_total',
                        'Total recommendation requests', ['endpoint'])
REQUEST_LATENCY = Histogram(
    'recommender_request_latency_seconds', 'Request latency', ['endpoint'])
PREDICTION_SCORE = Histogram(
    'recommender_prediction_score', 'Prediction scores')
ERROR_COUNT = Counter('recommender_errors_total', 'Total errors', ['type'])
ACTIVE_USERS = Gauge('recommender_active_users', 'Number of active users')

# Configuration
FEAST_URL = "http://feast-service:5003"  # Use Docker service name
FEAST_ENABLED = True  # Toggle Feast integration

# Load static data and model
print("Loading model and data...")
with open('/app/models/lightfm_model.pkl', 'rb') as f:
    model = pickle.load(f)

with open('/app/models/dataset.pkl', 'rb') as f:
    dataset = pickle.load(f)

movies_df = pd.read_csv('/app/data/processed/movies_clean.csv')
train_df = pd.read_csv('/app/data/processed/train.csv')

# Get dataset info
n_users, n_movies = dataset.interactions_shape()
print(f"Loaded: {n_users:,} users, {n_movies:,} movies")


def get_feast_features(user_idx: int) -> Dict[str, Any]:
    """
    Fetch features from Feast service

    Args:
        user_idx: User index

    Returns:
        Dictionary of features or empty dict if unavailable
    """
    if not FEAST_ENABLED:
        return {}

    try:
        response = requests.get(
            f"{FEAST_URL}/feast/user/{user_idx}",
            timeout=1  # Fast timeout for production
        )

        if response.status_code == 200:
            result = response.json()
            return result.get('features', {})
        else:
            print(f"Feast returned {response.status_code}")
            return {}

    except Exception as e:
        print(f"Feast error: {e}")
        ERROR_COUNT.labels(type='feast_unavailable').inc()
        return {}

# Create BentoML service


@bentoml.service(
    resources={"cpu": "2"},
    traffic={"timeout": 30},
)
class MovieRecommenderService:

    def __init__(self):
        self.model = model
        self.dataset = dataset
        self.movies_df = movies_df
        self.train_df = train_df
        self.n_users = n_users
        self.n_movies = n_movies

    @bentoml.api
    def health(self) -> Dict[str, Any]:
        """
        Health check endpoint

        Returns service status and model info
        """
        # Test Feast connection
        feast_healthy = False
        try:
            response = requests.get(f"{FEAST_URL}/health", timeout=2)
            feast_healthy = response.status_code == 200
        except:
            pass

        return {
            "status": "healthy",
            "model_loaded": self.model is not None,
            "total_users": int(self.n_users),
            "total_movies": int(self.n_movies),
            "feast_connected": feast_healthy,
            "feast_enabled": FEAST_ENABLED
        }

    @bentoml.api
    def metrics(self) -> str:
        """
        Prometheus metrics endpoint

        Returns metrics in Prometheus format
        """
        return generate_latest().decode('utf-8')

    @bentoml.api
    def recommend(
        self,
        user_idx: int,
        n_recommendations: int = 10,
        exclude_rated: bool = True,
        use_feast: bool = True
    ) -> Dict[str, Any]:
        """
        Generate movie recommendations for a user

        Args:
            user_idx: User index (0 to n_users-1)
            n_recommendations: Number of recommendations to return (1-100)
            exclude_rated: Whether to exclude movies the user has already rated
            use_feast: Whether to use Feast features (for boosting/filtering)

        Returns:
            Dictionary with user_idx, recommendations list, features, and count
        """
        start_time = time.time()
        REQUEST_COUNT.labels(endpoint='recommend').inc()

        try:
            # Validate inputs
            if user_idx < 0:
                ERROR_COUNT.labels(type='invalid_input').inc()
                raise ValueError(
                    f"User index must be non-negative, got {user_idx}")

            if user_idx >= self.n_users:
                ERROR_COUNT.labels(type='user_not_found').inc()
                raise ValueError(f"User index {user_idx} out of range. Max: {self.n_users-1}")

            if n_recommendations < 1 or n_recommendations > 100:
                ERROR_COUNT.labels(type='invalid_input').inc()
                raise ValueError(f"n_recommendations must be between 1 and 100, got {n_recommendations}")

            # Get Feast features (for context/boosting)
            feast_features = {}
            if use_feast and FEAST_ENABLED:
                feast_features = get_feast_features(user_idx)

            # Get movies already rated by user (if excluding)
            user_movies = set()
            if exclude_rated:
                user_movies = set(
                    self.train_df[self.train_df['user_idx'] == user_idx]['movie_idx'].values)

            # Predict scores for all movies
            user_ids = np.full(self.n_movies, user_idx)
            item_ids = np.arange(self.n_movies)
            scores = self.model.predict(
                user_ids=user_ids, item_ids=item_ids, num_threads=4)

            # Apply feature-based boosting if available
            if feast_features:
                # Boost recent activity - more active users get slight score boost
                recent_activity = feast_features.get('user_recent_activity', 0)
                if recent_activity > 0:
                    boost_factor = 1 + \
                        (min(recent_activity, 20) * 0.01)  # Max 20% boost
                    scores = scores * boost_factor

            # Create list of (movie_idx, score) for movies to recommend
            recommendations = []
            for movie_idx, score in enumerate(scores):
                if movie_idx not in user_movies:
                    recommendations.append((movie_idx, score))
                    PREDICTION_SCORE.observe(float(score))

            # Sort by score and get top N
            recommendations.sort(key=lambda x: x[1], reverse=True)
            top_recs = recommendations[:n_recommendations]

            # Get movie details
            results = []
            for movie_idx, score in top_recs:
                movie_info = self.movies_df[self.movies_df['movie_idx'] == movie_idx]
                if len(movie_info) > 0:
                    movie_info = movie_info.iloc[0]
                    results.append({
                        "movie_idx": int(movie_idx),
                        "movieId": int(movie_info['movieId']),
                        "title": str(movie_info['title']),
                        "genres": str(movie_info['genres']),
                        "score": float(score)
                    })

            latency = time.time() - start_time
            REQUEST_LATENCY.labels(endpoint='recommend').observe(latency)
            ACTIVE_USERS.set(user_idx)

            return {
                "user_idx": user_idx,
                "recommendations": results,
                "count": len(results),
                "feast_features": feast_features if use_feast else None,
                "latency_ms": round(latency * 1000, 2)
            }

        except Exception as e:
            ERROR_COUNT.labels(type='prediction_error').inc()
            raise

    @bentoml.api
    def batch_recommend(
        self,
        user_idx: int,
        n_recommendations: int = 10,
        exclude_rated: bool = True
    ) -> Dict[str, Any]:
        """
        Get recommendations with user history context

        Returns both recommendations and user's top-rated movies

        Args:
            user_idx: User index (0 to n_users-1)
            n_recommendations: Number of recommendations to return (1-100)
            exclude_rated: Whether to exclude movies the user has already rated

        Returns:
            Dictionary with user_idx, user_history, recommendations, and count
        """
        REQUEST_COUNT.labels(endpoint='batch_recommend').inc()

        # Get recommendations
        rec_response = self.recommend(
            user_idx, n_recommendations, exclude_rated)

        # Get user history
        user_history = self.train_df[self.train_df['user_idx'] == user_idx].merge(
            self.movies_df, on='movie_idx'
        )[['movieId', 'title', 'genres', 'rating']].sort_values('rating', ascending=False).head(10)

        return {
            "user_idx": user_idx,
            "user_history": user_history.to_dict(orient='records'),
            "recommendations": rec_response["recommendations"],
            "feast_features": rec_response.get("feast_features"),
            "count": rec_response["count"],
            "latency_ms": rec_response["latency_ms"]
        }
