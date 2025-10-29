FROM python:3.9-slim

WORKDIR /app

# Install system dependencies for building LightFM
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir \
    numpy \
    scipy \
    && pip install --no-cache-dir \
    lightfm \
    bentoml \
    pandas \
    scikit-learn \
    pydantic \
    requests \
    prometheus-client

# Copy service code
COPY service.py .

# Create directories
RUN mkdir -p /app/models /app/data

# Expose port
EXPOSE 3000

# Run BentoML service
CMD ["bentoml", "serve", "service:MovieRecommenderService", "--host", "0.0.0.0", "--port", "3000"]
