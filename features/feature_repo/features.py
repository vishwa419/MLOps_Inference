"""
Feast Feature View Definitions
Define all features for training and serving
"""

from feast import FeatureView, Field, FileSource
from feast.types import Float32, Int64, String
from datetime import timedelta
from entities import user, movie

# User batch features (offline store)
user_batch_source = FileSource(
    name="user_batch_source",
    path="../data/user_features.parquet",
    timestamp_field="event_timestamp"
)

user_batch_features = FeatureView(
    name="user_batch_features",
    entities=[user],
    ttl=timedelta(days=365),  # Features valid for 1 year
    schema=[
        Field(name="user_avg_rating", dtype=Float32),
        Field(name="user_rating_std", dtype=Float32),
        Field(name="user_rating_count", dtype=Int64),
        Field(name="user_min_rating", dtype=Float32),
        Field(name="user_max_rating", dtype=Float32),
        Field(name="user_rating_range", dtype=Float32),
        Field(name="user_days_active", dtype=Float32),
        Field(name="user_favorite_genre", dtype=String),
        Field(name="user_favorite_genre_count", dtype=Int64),
        Field(name="user_favorite_genre_avg_rating", dtype=Float32),
    ],
    source=user_batch_source,
    online=True,  # Enable online serving
    tags={"type": "batch", "entity": "user"}
)

# Movie batch features (offline store)
movie_batch_source = FileSource(
    name="movie_batch_source",
    path="../data/movie_features.parquet",
    timestamp_field="event_timestamp"
)

movie_batch_features = FeatureView(
    name="movie_batch_features",
    entities=[movie],
    ttl=timedelta(days=365),
    schema=[
        Field(name="movie_avg_rating", dtype=Float32),
        Field(name="movie_rating_std", dtype=Float32),
        Field(name="movie_rating_count", dtype=Int64),
        Field(name="movie_min_rating", dtype=Float32),
        Field(name="movie_max_rating", dtype=Float32),
        Field(name="movie_rating_range", dtype=Float32),
        Field(name="movie_popularity_score", dtype=Float32),
        Field(name="movie_quality_score", dtype=Float32),
    ],
    source=movie_batch_source,
    online=True,
    tags={"type": "batch", "entity": "movie"}
)

# User streaming features (online store - updated in real-time)
user_stream_source = FileSource(
    name="user_stream_source",
    path="../data/user_stream_features.parquet",
    timestamp_field="event_timestamp"
)

user_stream_features = FeatureView(
    name="user_stream_features",
    entities=[user],
    ttl=timedelta(hours=24),  # Short TTL for real-time features
    schema=[
        Field(name="user_recent_activity", dtype=Int64),
        Field(name="user_last_genre", dtype=String),
    ],
    source=user_stream_source,
    online=True,
    tags={"type": "stream", "entity": "user"}
)

# Movie streaming features (online store)
movie_stream_source = FileSource(
    name="movie_stream_source",
    path="../data/movie_stream_features.parquet",
    timestamp_field="event_timestamp"
)

movie_stream_features = FeatureView(
    name="movie_stream_features",
    entities=[movie],
    ttl=timedelta(hours=24),
    schema=[
        Field(name="movie_popularity", dtype=Int64),
        Field(name="movie_recent_views", dtype=Int64),
    ],
    source=movie_stream_source,
    online=True,
    tags={"type": "stream", "entity": "movie"}
)
