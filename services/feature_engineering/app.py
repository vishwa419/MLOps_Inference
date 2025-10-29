"""
Feature Engineering Microservice API
Computes and serves batch and streaming features
"""

from fastapi import FastAPI, HTTPException, File, UploadFile
from pydantic import BaseModel, Field
from typing import Dict, Any, Optional
import pandas as pd
import io
import os
from datetime import datetime

from feature_engineer import FeatureEngineer

# Initialize FastAPI
app = FastAPI(
    title="Feature Engineering Service",
    description="Computes batch and streaming features for ML models",
    version="1.0.0"
)

# Initialize feature engineer
engineer = FeatureEngineer()

# Request/Response Models
class BatchFeatureRequest(BaseModel):
    ratings_path: str = Field(..., description="Path to ratings CSV")
    movies_path: str = Field(..., description="Path to movies CSV")
    export_to_feast: bool = Field(default=True, description="Export to Feast format")

class BatchFeatureResponse(BaseModel):
    success: bool
    features_computed: Dict[str, int]  # feature_name -> row_count
    output_paths: Dict[str, str]
    timestamp: str

class StreamEventRequest(BaseModel):
    user_idx: int = Field(..., ge=0)
    movie_idx: int = Field(..., ge=0)
    event_type: str = Field(..., description="Type: rating, view, click, search")
    timestamp: float
    genres: Optional[str] = None

class StreamFeatureResponse(BaseModel):
    success: bool
    features: Dict[str, Any]
    timestamp: str

class UserFeaturesRequest(BaseModel):
    user_idx: int = Field(..., ge=0)

class MovieFeaturesRequest(BaseModel):
    movie_idx: int = Field(..., ge=0)

class HealthResponse(BaseModel):
    status: str
    service: str
    stream_features_cached: Dict[str, int]
    timestamp: str

# API Endpoints

@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "service": "Feature Engineering Service",
        "version": "1.0.0",
        "endpoints": {
            "batch": "/features/batch",
            "stream": "/features/stream",
            "user": "/features/user/{user_idx}",
            "movie": "/features/movie/{movie_idx}",
            "health": "/health"
        }
    }

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint"""
    return HealthResponse(
        status="healthy",
        service="feature-engineering-service",
        stream_features_cached={
            "users": len(engineer.stream_features['user_recent_activity']),
            "movies": len(engineer.stream_features['movie_popularity'])
        },
        timestamp=datetime.now().isoformat()
    )

@app.post("/features/batch", response_model=BatchFeatureResponse)
async def compute_batch_features(request: BatchFeatureRequest):
    """
    Compute batch features from historical data
    
    Example:
    ```json
    {
        "ratings_path": "data/processed/train.csv",
        "movies_path": "data/processed/movies_clean.csv",
        "export_to_feast": true
    }
    ```
    """
    try:
        # Check files exist
        if not os.path.exists(request.ratings_path):
            raise HTTPException(status_code=404, detail=f"Ratings file not found: {request.ratings_path}")
        
        if not os.path.exists(request.movies_path):
            raise HTTPException(status_code=404, detail=f"Movies file not found: {request.movies_path}")
        
        # Load data
        print(f"Loading data from {request.ratings_path} and {request.movies_path}")
        ratings_df = pd.read_csv(request.ratings_path)
        movies_df = pd.read_csv(request.movies_path)
        
        # Compute features
        print("Computing batch features...")
        features = engineer.compute_batch_features(ratings_df, movies_df)
        
        # Export to Feast format
        output_paths = {}
        if request.export_to_feast:
            output_dir = engineer.export_features_to_feast(features)
            for feature_name in features.keys():
                output_paths[feature_name] = f"{output_dir}/{feature_name}.parquet"
        
        # Prepare response
        features_computed = {
            name: len(df) for name, df in features.items()
        }
        
        return BatchFeatureResponse(
            success=True,
            features_computed=features_computed,
            output_paths=output_paths,
            timestamp=datetime.now().isoformat()
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/features/stream", response_model=StreamFeatureResponse)
async def update_stream_features(event: StreamEventRequest):
    """
    Update streaming features based on incoming event
    
    Example:
    ```json
    {
        "user_idx": 100,
        "movie_idx": 50,
        "event_type": "view",
        "timestamp": 1698765432.0,
        "genres": "Action|Thriller"
    }
    ```
    """
    try:
        # Convert to dict
        event_dict = event.dict()
        
        # Update features
        updated_features = engineer.update_stream_features(event_dict)
        
        return StreamFeatureResponse(
            success=True,
            features=updated_features,
            timestamp=datetime.now().isoformat()
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/features/user/{user_idx}")
async def get_user_features(user_idx: int):
    """Get streaming features for a specific user"""
    try:
        features = engineer.get_user_stream_features(user_idx)
        return {
            "success": True,
            "features": features,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/features/movie/{movie_idx}")
async def get_movie_features(movie_idx: int):
    """Get streaming features for a specific movie"""
    try:
        features = engineer.get_movie_stream_features(movie_idx)
        return {
            "success": True,
            "features": features,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/features/upload")
async def compute_features_from_upload(
    ratings_file: UploadFile = File(...),
    movies_file: UploadFile = File(...)
):
    """
    Upload CSV files and compute features
    
    Args:
        ratings_file: Ratings CSV
        movies_file: Movies CSV
    """
    try:
        # Read uploaded files
        ratings_content = await ratings_file.read()
        movies_content = await movies_file.read()
        
        ratings_df = pd.read_csv(io.BytesIO(ratings_content))
        movies_df = pd.read_csv(io.BytesIO(movies_content))
        
        # Compute features
        features = engineer.compute_batch_features(ratings_df, movies_df)
        
        # Export
        output_dir = engineer.export_features_to_feast(features)
        
        features_computed = {
            name: len(df) for name, df in features.items()
        }
        
        return {
            "success": True,
            "features_computed": features_computed,
            "output_dir": output_dir,
            "timestamp": datetime.now().isoformat()
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/features/stats")
async def get_feature_stats():
    """Get statistics about computed features"""
    return {
        "stream_features": {
            "active_users": len(engineer.stream_features['user_recent_activity']),
            "active_movies": len(engineer.stream_features['movie_popularity']),
            "total_user_events": sum(engineer.stream_features['user_recent_activity'].values()),
            "total_movie_views": sum(engineer.stream_features['movie_popularity'].values())
        },
        "timestamp": datetime.now().isoformat()
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5002)
