"""
Validation Microservice API
FastAPI wrapper around Great Expectations
"""

from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.responses import HTMLResponse, FileResponse
from pydantic import BaseModel, Field
from typing import Dict, Any, Optional
import pandas as pd
import io
import os
from datetime import datetime

from validator import DataValidator

# Initialize FastAPI
app = FastAPI(
    title="Data Validation Service",
    description="Validates batch and streaming data using Great Expectations",
    version="1.0.0"
)

# Initialize validator
validator = DataValidator()

# Request/Response Models
class BatchValidationRequest(BaseModel):
    file_path: str = Field(..., description="Path to CSV file")
    data_type: str = Field(..., description="Type: 'ratings' or 'movies'")

class BatchValidationResponse(BaseModel):
    valid: bool
    evaluated_expectations: int
    successful_expectations: int
    failed_expectations: int
    success_rate: float
    failures: list
    timestamp: str
    report_url: Optional[str] = None

class StreamEventValidationRequest(BaseModel):
    user_idx: int = Field(..., ge=0)
    movie_idx: int = Field(..., ge=0)
    event_type: str = Field(..., description="Type: rating, view, click, search")
    timestamp: float
    rating: Optional[float] = Field(None, ge=0.5, le=5.0)

class StreamValidationResponse(BaseModel):
    valid: bool
    errors: list
    event: dict

class HealthResponse(BaseModel):
    status: str
    service: str
    timestamp: str

# API Endpoints

@app.get("/", response_class=HTMLResponse)
async def root():
    """Root endpoint with service info"""
    return """
    <html>
        <head><title>Validation Service</title></head>
        <body>
            <h1>Data Validation Service</h1>
            <p>FastAPI + Great Expectations</p>
            <h2>Endpoints:</h2>
            <ul>
                <li>POST /validate/batch - Validate CSV files</li>
                <li>POST /validate/stream - Validate single events</li>
                <li>POST /validate/upload - Upload and validate CSV</li>
                <li>GET /health - Health check</li>
                <li>GET /docs - API documentation</li>
            </ul>
        </body>
    </html>
    """

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint"""
    return HealthResponse(
        status="healthy",
        service="validation-service",
        timestamp=datetime.now().isoformat()
    )

@app.post("/validate/batch", response_model=BatchValidationResponse)
async def validate_batch(request: BatchValidationRequest):
    """
    Validate batch data (CSV files)
    
    Example:
    ```json
    {
        "file_path": "/data/train.csv",
        "data_type": "ratings"
    }
    ```
    """
    try:
        # Check file exists
        if not os.path.exists(request.file_path):
            raise HTTPException(status_code=404, detail=f"File not found: {request.file_path}")
        
        # Load data
        df = pd.read_csv(request.file_path)
        
        # Validate based on type
        if request.data_type == "ratings":
            results = validator.validate_ratings_batch(df)
        elif request.data_type == "movies":
            results = validator.validate_movies_batch(df)
        else:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid data_type: {request.data_type}. Must be 'ratings' or 'movies'"
            )
        
        # Generate HTML report
        report_path = f"reports/{request.data_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
        report_url = validator.generate_report(results, report_path)
        results["report_url"] = f"/reports/{os.path.basename(report_url)}"
        
        return BatchValidationResponse(**results)
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/validate/upload", response_model=BatchValidationResponse)
async def validate_upload(
    file: UploadFile = File(...),
    data_type: str = "ratings"
):
    """
    Upload and validate a CSV file
    
    Args:
        file: CSV file upload
        data_type: 'ratings' or 'movies'
    """
    try:
        # Read uploaded file
        contents = await file.read()
        df = pd.read_csv(io.BytesIO(contents))
        
        # Validate
        if data_type == "ratings":
            results = validator.validate_ratings_batch(df)
        elif data_type == "movies":
            results = validator.validate_movies_batch(df)
        else:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid data_type: {data_type}"
            )
        
        # Generate report
        report_path = f"reports/upload_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
        report_url = validator.generate_report(results, report_path)
        results["report_url"] = f"/reports/{os.path.basename(report_url)}"
        
        return BatchValidationResponse(**results)
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/validate/stream", response_model=StreamValidationResponse)
async def validate_stream(event: StreamEventValidationRequest):
    """
    Validate a single streaming event
    
    Example:
    ```json
    {
        "user_idx": 100,
        "movie_idx": 50,
        "event_type": "rating",
        "timestamp": 1698765432.0,
        "rating": 4.5
    }
    ```
    """
    try:
        # Convert to dict
        event_dict = event.dict()
        
        # Validate
        results = validator.validate_stream_event(event_dict)
        
        return StreamValidationResponse(**results)
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/reports/{filename}")
async def get_report(filename: str):
    """Serve validation report HTML"""
    report_path = f"reports/{filename}"
    
    if not os.path.exists(report_path):
        raise HTTPException(status_code=404, detail="Report not found")
    
    return FileResponse(report_path, media_type="text/html")

@app.get("/reports")
async def list_reports():
    """List all validation reports"""
    if not os.path.exists("reports"):
        return {"reports": []}
    
    reports = []
    for filename in os.listdir("reports"):
        if filename.endswith(".html"):
            reports.append({
                "filename": filename,
                "url": f"/reports/{filename}",
                "created": datetime.fromtimestamp(
                    os.path.getctime(f"reports/{filename}")
                ).isoformat()
            })
    
    reports.sort(key=lambda x: x['created'], reverse=True)
    return {"reports": reports}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5001)
