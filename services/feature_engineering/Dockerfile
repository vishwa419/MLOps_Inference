FROM python:3.9-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY feature_engineer.py .
COPY app.py .

# Create directories
RUN mkdir -p features/data

# Expose port
EXPOSE 5002

# Run the application
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "5002"]
