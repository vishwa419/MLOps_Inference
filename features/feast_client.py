"""
Feast Client Wrapper
Provides easy interface to Feast feature store
"""

from feast import FeatureStore
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Any
import numpy as np


class FeastClient:
    """Client for interacting with Feast feature store"""
    
    def __init__(self, repo_path: str = "features/feature_repo"):
        """Initialize Feast store"""
        self.store = FeatureStore(repo_path=repo_path)
        print(f"Initialized Feast store from {repo_path}")
    
    def apply_feature_definitions(self):
        """Apply feature definitions to registry"""
        print("Applying feature definitions to Feast...")
        self.store.apply([
            # This will register all entities and feature views
        ])
        print("Feature definitions applied successfully")
    
    def materialize_batch_features(
        self, 
        start_date: datetime = None,
        end_date: datetime = None
    ):
        """
        Materialize batch features to online store (Redis)
        This makes features available for real-time serving
        
        Args:
            start_date: Start of time range
            end_date: End of time range (default: now)
        """
        if end_date is None:
            end_date = datetime.now()
        
        if start_date is None:
            start_date = end_date - timedelta(days=365)  # Last year
        
        print(f"Materializing features from {start_date} to {end_date}...")
        
        # Materialize user batch features
        self.store.materialize(
            start_date=start_date,
            end_date=end_date,
            feature_views=["user_batch_features", "movie_batch_features"]
        )
        
        print("Batch features materialized to Redis")
    
    def materialize_stream_features(self):
        """Materialize streaming features to online store"""
        end_date = datetime.now()
        start_date = end_date - timedelta(hours=24)
        
        print(f"Materializing stream features from {start_date} to {end_date}...")
        
        self.store.materialize(
            start_date=start_date,
            end_date=end_date,
            feature_views=["user_stream_features", "movie_stream_features"]
        )
        
        print("Stream features materialized to Redis")
    
    def get_online_features(
        self, 
        user_ids: List[int] = None,
        movie_ids: List[int] = None,
        feature_refs: List[str] = None
    ) -> pd.DataFrame:
        """
        Get features from online store (Redis) for real-time serving
        
        Args:
            user_ids: List of user IDs
            movie_ids: List of movie IDs
            feature_refs: List of feature references (e.g., "user_batch_features:user_avg_rating")
            
        Returns:
            DataFrame with features
        """
        entity_rows = []
        
        if user_ids:
            entity_rows = [{"user_idx": uid} for uid in user_ids]
        elif movie_ids:
            entity_rows = [{"movie_idx": mid} for mid in movie_ids]
        
        if feature_refs is None:
            feature_refs = self._get_all_feature_refs()
        
        features = self.store.get_online_features(
            features=feature_refs,
            entity_rows=entity_rows
        ).to_df()
        
        return features
    
    def get_historical_features(
        self,
        entity_df: pd.DataFrame,
        feature_refs: List[str] = None
    ) -> pd.DataFrame:
        """
        Get historical features for training
        
        Args:
            entity_df: DataFrame with entity IDs and timestamps
            feature_refs: List of feature references
            
        Returns:
            DataFrame with historical features
        """
        if feature_refs is None:
            feature_refs = self._get_all_feature_refs()
        
        training_df = self.store.get_historical_features(
            entity_df=entity_df,
            features=feature_refs
        ).to_df()
        
        return training_df
    
    def push_stream_features(
        self,
        feature_view_name: str,
        features_df: pd.DataFrame
    ):
        """
        Push streaming features directly to online store
        Used for real-time feature updates
        
        Args:
            feature_view_name: Name of feature view
            features_df: DataFrame with features to push
        """
        self.store.push(
            push_source_name=feature_view_name,
            df=features_df,
            to="online"
        )
        print(f"Pushed {len(features_df)} rows to {feature_view_name}")
    
    def _get_all_feature_refs(self) -> List[str]:
        """Get all feature references"""
        feature_refs = []
        
        for fv in self.store.list_feature_views():
            for field in fv.schema:
                feature_refs.append(f"{fv.name}:{field.name}")
        
        return feature_refs
    
    def get_user_features_for_serving(self, user_id: int) -> Dict[str, Any]:
        """
        Get all features for a user (batch + stream) for serving
        
        Args:
            user_id: User ID
            
        Returns:
            Dictionary of features
        """
        features = self.get_online_features(
            user_ids=[user_id],
            feature_refs=[
                # Batch features
                "user_batch_features:user_avg_rating",
                "user_batch_features:user_rating_count",
                "user_batch_features:user_favorite_genre",
                # Stream features
                "user_stream_features:user_recent_activity",
                "user_stream_features:user_last_genre"
            ]
        )
        
        return features.iloc[0].to_dict() if len(features) > 0 else {}
    
    def get_movie_features_for_serving(self, movie_id: int) -> Dict[str, Any]:
        """
        Get all features for a movie (batch + stream) for serving
        
        Args:
            movie_id: Movie ID
            
        Returns:
            Dictionary of features
        """
        features = self.get_online_features(
            movie_ids=[movie_id],
            feature_refs=[
                # Batch features
                "movie_batch_features:movie_avg_rating",
                "movie_batch_features:movie_rating_count",
                "movie_batch_features:movie_popularity_score",
                # Stream features
                "movie_stream_features:movie_popularity",
                "movie_stream_features:movie_recent_views"
            ]
        )
        
        return features.iloc[0].to_dict() if len(features) > 0 else {}
    
    def update_stream_feature_from_event(self, event: Dict[str, Any]):
        """
        Update streaming features based on Kafka event
        
        Args:
            event: Event dictionary from Kafka
        """
        # Update user stream features
        user_stream_df = pd.DataFrame([{
            "user_idx": event["user_idx"],
            "event_timestamp": datetime.fromtimestamp(event["timestamp"]),
            "user_recent_activity": event.get("user_recent_activity", 1),
            "user_last_genre": event.get("genres", "Unknown")
        }])
        
        self.push_stream_features("user_stream_features", user_stream_df)
        
        # Update movie stream features
        if event["event_type"] in ["view", "click"]:
            movie_stream_df = pd.DataFrame([{
                "movie_idx": event["movie_idx"],
                "event_timestamp": datetime.fromtimestamp(event["timestamp"]),
                "movie_popularity": event.get("movie_popularity", 1),
                "movie_recent_views": event.get("movie_recent_views", 1)
            }])
            
            self.push_stream_features("movie_stream_features", movie_stream_df)
    
    def get_feature_stats(self) -> Dict[str, Any]:
        """Get statistics about features in the store"""
        stats = {
            "feature_views": [],
            "entities": []
        }
        
        for fv in self.store.list_feature_views():
            stats["feature_views"].append({
                "name": fv.name,
                "num_features": len(fv.schema),
                "ttl_hours": fv.ttl.total_seconds() / 3600 if fv.ttl else None,
                "online": fv.online,
                "tags": fv.tags
            })
        
        for entity in self.store.list_entities():
            stats["entities"].append({
                "name": entity.name,
                "join_keys": entity.join_keys
            })
        
        return stats
