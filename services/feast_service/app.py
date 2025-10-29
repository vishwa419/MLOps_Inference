"""
Feast Feature Store Service API
Provides REST API for Feast operations
"""

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
from datetime import datetime
import sys
sys.path.append('/app/features')

from feast_client import FeastClient

# Initialize FastAPI
app = FastAPI(
    title="Feast Feature Store Service",
    description="Feature store management and serving",
    version="1.0.0"
)

# Initialize Feast client
feast_client = FeastClient(repo_path="/app/features/feature_repo")

# Request/Response Models
class MaterializeRequest(BaseModel):
    feature_views: Optional[List[str]] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None

class OnlineFeaturesRequest(BaseModel):
    user_ids: Optional[List[int]] = None
    movie_ids: Optional[List[int]] = None
    feature_refs: Optional[List[str]] = None

class StreamFeatureUpdateRequest(BaseModel):
    event: Dict[str, Any]

class HealthResponse(BaseModel):
    status: str
    service: str
    feast_registry_connected: bool
    redis_connected: bool
    timestamp: str

# API Endpoints

@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "service": "Feast Feature Store Service",
        "version": "1.0.0",
        "endpoints": {
            "materialize": "/feast/materialize",
            "online_features": "/feast/online_features",
            "user_features": "/feast/user/{user_id}",
            "movie_features": "/feast/movie/{movie_id}",
            "stats": "/feast/stats",
            "health": "/health"
        }
    }

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint"""
    try:
        # Test Feast connection
        stats = feast_client.get_feature_stats()
        feast_connected = True
        redis_connected = True  # If feast works, Redis works
    except Exception as e:
        feast_connected = False
        redis_connected = False
    
    return HealthResponse(
        status="healthy" if feast_connected else "degraded",
        service="feast-service",
        feast_registry_connected=feast_connected,
        redis_connected=redis_connected,
        timestamp=datetime.now().isoformat()
    )

@app.post("/feast/materialize")
async def materialize_features(request: MaterializeRequest):
    """
    Materialize features from offline to online store (Redis)
    
    Example:
    ```json
    {
        "feature_views": ["user_batch_features", "movie_batch_features"],
        "end_date": "2024-01-01T00:00:00"
    }
    ```
    """
    try:
        start_date = datetime.fromisoformat(request.start_date) if request.start_date else None
        end_date = datetime.fromisoformat(request.end_date) if request.end_date else None
        
        feast_client.materialize_batch_features(
            start_date=start_date,
            end_date=end_date
        )
        
        return {
            "success": True,
            "message": "Features materialized to Redis",
            "timestamp": datetime.now().isoformat()
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/feast/materialize/stream")
async def materialize_stream_features():
    """Materialize streaming features to online store"""
    try:
        feast_client.materialize_stream_features()
        
        return {
            "success": True,
            "message": "Stream features materialized to Redis",
            "timestamp": datetime.now().isoformat()
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/feast/online_features")
async def get_online_features(request: OnlineFeaturesRequest):
    """
    Get features from online store (Redis) for serving
    
    Example:
    ```json
    {
        "user_ids": [100, 200, 300],
        "feature_refs": [
            "user_batch_features:user_avg_rating",
            "user_stream_features:user_recent_activity"
        ]
    }
    ```
    """
    try:
        features_df = feast_client.get_online_features(
            user_ids=request.user_ids,
            movie_ids=request.movie_ids,
            feature_refs=request.feature_refs
        )
        
        return {
            "success": True,
            "features": features_df.to_dict(orient="records"),
            "count": len(features_df),
            "timestamp": datetime.now().isoformat()
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/feast/user/{user_id}")
async def get_user_features(user_id: int):
    """Get all features for a specific user"""
    try:
        features = feast_client.get_user_features_for_serving(user_id)
        
        return {
            "success": True,
            "user_id": user_id,
            "features": features,
            "timestamp": datetime.now().isoformat()
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/feast/movie/{movie_id}")
async def get_movie_features(movie_id: int):
    """Get all features for a specific movie"""
    try:
        features = feast_client.get_movie_features_for_serving(movie_id)
        
        return {
            "success": True,
            "movie_id": movie_id,
            "features": features,
            "timestamp": datetime.now().isoformat()
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/feast/stream/update")
async def update_stream_feature(request: StreamFeatureUpdateRequest):
    """
    Update streaming features from Kafka event
    
    Example:
    ```json
    {
        "event": {
            "user_idx": 100,
            "movie_idx": 50,
            "event_type": "view",
            "timestamp": 1698765432.0,
            "user_recent_activity": 15,
            "movie_popularity": 230
        }
    }
    ```
    """
    try:
        feast_client.update_stream_feature_from_event(request.event)
        
        return {
            "success": True,
            "message": "Stream features updated in Redis",
            "timestamp": datetime.now().isoformat()
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/feast/stats")
async def get_feature_store_stats():
    """Get statistics about the feature store"""
    try:
        stats = feast_client.get_feature_stats()
        
        return {
            "success": True,
            "stats": stats,
            "timestamp": datetime.now().isoformat()
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5003)
