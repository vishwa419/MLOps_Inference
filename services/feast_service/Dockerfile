FROM python:3.9-slim

WORKDIR /app

# Install dependencies
COPY services/feast_service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy Feast repo and client
COPY features /app/features

# Copy application code
COPY services/feast_service/app.py .

# Create data directory
RUN mkdir -p /app/features/data

# Expose port
EXPOSE 5003

# Run the application
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "5003"]
