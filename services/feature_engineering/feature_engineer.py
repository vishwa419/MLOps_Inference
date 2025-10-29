"""
Feature Engineering Core Logic
Computes batch and streaming features
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, List
from datetime import datetime, timedelta
from collections import defaultdict
import json


class FeatureEngineer:
    """Handles feature computation for batch and streaming data"""
    
    def __init__(self):
        """Initialize feature engineer"""
        # In-memory store for streaming features (in production, this would be Redis)
        self.stream_features = {
            'user_recent_activity': defaultdict(int),
            'movie_popularity': defaultdict(int),
            'user_last_genre': {},
            'movie_recent_views': defaultdict(int)
        }
        self.feature_window = timedelta(hours=24)  # 24 hour window for streaming features
    
    def compute_batch_features(self, ratings_df: pd.DataFrame, movies_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """
        Compute batch features from historical data
        
        Args:
            ratings_df: DataFrame with [user_idx, movie_idx, rating, timestamp]
            movies_df: DataFrame with [movie_idx, movieId, title, genres]
            
        Returns:
            Dictionary of feature DataFrames: {
                'user_features': DataFrame,
                'movie_features': DataFrame,
                'user_movie_features': DataFrame
            }
        """
        print("Computing batch features...")
        
        # User features
        user_features = self._compute_user_features(ratings_df)
        
        # Movie features
        movie_features = self._compute_movie_features(ratings_df, movies_df)
        
        # User-movie interaction features
        user_movie_features = self._compute_user_movie_features(ratings_df, movies_df)
        
        return {
            'user_features': user_features,
            'movie_features': movie_features,
            'user_movie_features': user_movie_features
        }
    
    def _compute_user_features(self, ratings_df: pd.DataFrame) -> pd.DataFrame:
        """Compute user-level features"""
        
        user_features = ratings_df.groupby('user_idx').agg({
            'rating': ['mean', 'std', 'count', 'min', 'max'],
            'timestamp': ['min', 'max']
        }).reset_index()
        
        # Flatten column names
        user_features.columns = [
            'user_idx',
            'user_avg_rating',
            'user_rating_std',
            'user_rating_count',
            'user_min_rating',
            'user_max_rating',
            'user_first_rating_time',
            'user_last_rating_time'
        ]
        
        # Additional features
        user_features['user_rating_range'] = user_features['user_max_rating'] - user_features['user_min_rating']
        user_features['user_days_active'] = (
            user_features['user_last_rating_time'] - user_features['user_first_rating_time']
        ) / 86400  # Convert to days
        
        # Fill NaN std with 0 (users with only 1 rating)
        user_features['user_rating_std'].fillna(0, inplace=True)
        
        return user_features
    
    def _compute_movie_features(self, ratings_df: pd.DataFrame, movies_df: pd.DataFrame) -> pd.DataFrame:
        """Compute movie-level features"""
        
        movie_features = ratings_df.groupby('movie_idx').agg({
            'rating': ['mean', 'std', 'count', 'min', 'max'],
            'timestamp': ['min', 'max']
        }).reset_index()
        
        # Flatten column names
        movie_features.columns = [
            'movie_idx',
            'movie_avg_rating',
            'movie_rating_std',
            'movie_rating_count',
            'movie_min_rating',
            'movie_max_rating',
            'movie_first_rating_time',
            'movie_last_rating_time'
        ]
        
        # Additional features
        movie_features['movie_rating_range'] = movie_features['movie_max_rating'] - movie_features['movie_min_rating']
        
        # Popularity score (log-scaled rating count)
        movie_features['movie_popularity_score'] = np.log1p(movie_features['movie_rating_count'])
        
        # Quality score (weighted rating)
        # Bayesian average: (C * m + R * v) / (C + v)
        # C = average rating count, m = overall average rating, R = movie rating, v = movie rating count
        C = movie_features['movie_rating_count'].mean()
        m = ratings_df['rating'].mean()
        
        movie_features['movie_quality_score'] = (
            (C * m + movie_features['movie_avg_rating'] * movie_features['movie_rating_count']) / 
            (C + movie_features['movie_rating_count'])
        )
        
        # Fill NaN
        movie_features['movie_rating_std'].fillna(0, inplace=True)
        
        # Merge with movie metadata
        movie_features = movie_features.merge(
            movies_df[['movie_idx', 'genres']], 
            on='movie_idx', 
            how='left'
        )
        
        # Genre features (one-hot encoding)
        genre_features = self._encode_genres(movie_features['genres'])
        movie_features = pd.concat([movie_features, genre_features], axis=1)
        
        return movie_features
    
    def _compute_user_movie_features(self, ratings_df: pd.DataFrame, movies_df: pd.DataFrame) -> pd.DataFrame:
        """Compute user-movie interaction features"""
        
        # Merge ratings with genres
        ratings_with_genres = ratings_df.merge(
            movies_df[['movie_idx', 'genres']], 
            on='movie_idx', 
            how='left'
        )
        
        # User's favorite genres (most rated genres)
        user_genre_counts = []
        
        for user_idx, group in ratings_with_genres.groupby('user_idx'):
            genre_counts = defaultdict(int)
            genre_ratings = defaultdict(list)
            
            for _, row in group.iterrows():
                if pd.notna(row['genres']) and row['genres'] != '(no genres listed)':
                    genres = row['genres'].split('|')
                    for genre in genres:
                        genre_counts[genre] += 1
                        genre_ratings[genre].append(row['rating'])
            
            if genre_counts:
                top_genre = max(genre_counts, key=genre_counts.get)
                top_genre_count = genre_counts[top_genre]
                top_genre_avg_rating = np.mean(genre_ratings[top_genre])
            else:
                top_genre = 'Unknown'
                top_genre_count = 0
                top_genre_avg_rating = 0
            
            user_genre_counts.append({
                'user_idx': user_idx,
                'user_favorite_genre': top_genre,
                'user_favorite_genre_count': top_genre_count,
                'user_favorite_genre_avg_rating': top_genre_avg_rating
            })
        
        user_genre_df = pd.DataFrame(user_genre_counts)
        
        return user_genre_df
    
    def _encode_genres(self, genres_series: pd.Series) -> pd.DataFrame:
        """One-hot encode movie genres"""
        
        # Get all unique genres
        all_genres = set()
        for genres in genres_series:
            if pd.notna(genres) and genres != '(no genres listed)':
                all_genres.update(genres.split('|'))
        
        all_genres = sorted(list(all_genres))
        
        # Create one-hot encoding
        genre_dict = {f'genre_{genre}': [] for genre in all_genres}
        
        for genres in genres_series:
            if pd.notna(genres) and genres != '(no genres listed)':
                movie_genres = set(genres.split('|'))
            else:
                movie_genres = set()
            
            for genre in all_genres:
                genre_dict[f'genre_{genre}'].append(1 if genre in movie_genres else 0)
        
        return pd.DataFrame(genre_dict)
    
    def update_stream_features(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Update streaming features based on incoming event
        
        Args:
            event: Event dictionary with fields from Kafka
            
        Returns:
            Updated feature values for the user/movie
        """
        user_idx = event['user_idx']
        movie_idx = event['movie_idx']
        event_type = event['event_type']
        
        # Update user recent activity
        self.stream_features['user_recent_activity'][user_idx] += 1
        
        # Update movie popularity
        if event_type in ['view', 'click']:
            self.stream_features['movie_popularity'][movie_idx] += 1
            self.stream_features['movie_recent_views'][movie_idx] += 1
        
        # Update user's last genre (if available in event)
        if 'genres' in event:
            self.stream_features['user_last_genre'][user_idx] = event['genres']
        
        # Return updated features
        return {
            'user_idx': user_idx,
            'movie_idx': movie_idx,
            'user_recent_activity': self.stream_features['user_recent_activity'][user_idx],
            'movie_popularity': self.stream_features['movie_popularity'][movie_idx],
            'movie_recent_views': self.stream_features['movie_recent_views'][movie_idx],
            'user_last_genre': self.stream_features['user_last_genre'].get(user_idx, 'Unknown'),
            'timestamp': datetime.now().isoformat()
        }
    
    def get_user_stream_features(self, user_idx: int) -> Dict[str, Any]:
        """Get streaming features for a user"""
        return {
            'user_idx': user_idx,
            'user_recent_activity': self.stream_features['user_recent_activity'].get(user_idx, 0),
            'user_last_genre': self.stream_features['user_last_genre'].get(user_idx, 'Unknown')
        }
    
    def get_movie_stream_features(self, movie_idx: int) -> Dict[str, Any]:
        """Get streaming features for a movie"""
        return {
            'movie_idx': movie_idx,
            'movie_popularity': self.stream_features['movie_popularity'].get(movie_idx, 0),
            'movie_recent_views': self.stream_features['movie_recent_views'].get(movie_idx, 0)
        }
    
    def export_features_to_feast(self, features: Dict[str, pd.DataFrame], output_dir: str = 'features/data'):
        """
        Export computed features to parquet files for Feast ingestion
        
        Args:
            features: Dictionary of feature DataFrames
            output_dir: Directory to save parquet files
        """
        import os
        from datetime import datetime
        
        os.makedirs(output_dir, exist_ok=True)
        
        # Add event_timestamp column (required by Feast)
        current_time = datetime.now()
        
        for feature_name, df in features.items():
            # Add timestamp if not present
            if 'event_timestamp' not in df.columns:
                df['event_timestamp'] = current_time
            
            output_path = f"{output_dir}/{feature_name}.parquet"
            df.to_parquet(output_path, index=False)
            print(f"Saved {feature_name} to {output_path}")
        
        return output_dir
    
    def export_stream_features_to_feast(self, output_dir: str = 'features/data'):
        """
        Export current streaming features to parquet for Feast
        
        Args:
            output_dir: Directory to save parquet files
        """
        import os
        from datetime import datetime
        
        os.makedirs(output_dir, exist_ok=True)
        current_time = datetime.now()
        
        # User stream features
        user_stream_data = []
        for user_idx, activity in self.stream_features['user_recent_activity'].items():
            user_stream_data.append({
                'user_idx': user_idx,
                'user_recent_activity': activity,
                'user_last_genre': self.stream_features['user_last_genre'].get(user_idx, 'Unknown'),
                'event_timestamp': current_time
            })
        
        if user_stream_data:
            user_stream_df = pd.DataFrame(user_stream_data)
            user_stream_df.to_parquet(f"{output_dir}/user_stream_features.parquet", index=False)
            print(f"Saved user stream features: {len(user_stream_data)} users")
        
        # Movie stream features
        movie_stream_data = []
        for movie_idx, popularity in self.stream_features['movie_popularity'].items():
            movie_stream_data.append({
                'movie_idx': movie_idx,
                'movie_popularity': popularity,
                'movie_recent_views': self.stream_features['movie_recent_views'].get(movie_idx, 0),
                'event_timestamp': current_time
            })
        
        if movie_stream_data:
            movie_stream_df = pd.DataFrame(movie_stream_data)
            movie_stream_df.to_parquet(f"{output_dir}/movie_stream_features.parquet", index=False)
            print(f"Saved movie stream features: {len(movie_stream_data)} movies")
        
        return output_dir
