# MLOps Inference Pipeline: Streaming Recommendation System

<div align="center">

![MLOps](https://img.shields.io/badge/MLOps-Production-blue)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker)
![Python](https://img.shields.io/badge/Python-3.9+-3776AB?logo=python)
![License](https://img.shields.io/badge/License-MIT-green)

**A production-ready MLOps inference pipeline for real-time movie recommendations**

[Architecture](#architecture) • [Features](#features) • [Quick Start](#quick-start) • [Services](#services) • [Performance](#performance)

</div>

---

## 📖 Overview

This project demonstrates a complete MLOps inference pipeline for a streaming recommendation system. It showcases best practices for deploying machine learning models in production, including real-time feature serving, data validation, monitoring, and event streaming.

**Key Capabilities:**
- ⚡ Real-time predictions (<500ms response time)
- 🔄 Streaming feature updates (sub-second latency)
- ✅ Automated data validation
- 📊 Comprehensive monitoring and observability
- 🚀 Scalable microservices architecture
- 📦 Fully containerized with Docker Compose

---

## 🏗️ Architecture

The system follows a microservices architecture with specialized components:

```
┌─────────────────┐
│   User Request  │
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────────────────────┐
│           🚪 Validation Service (5001)          │
│         Great Expectations - Data Quality       │
└────────┬────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────┐
│          🛋️  BentoML Service (3000)             │
│         Model Serving & Predictions             │
└────────┬────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────┐
│          🍳 Feast Service (5003)                │
│         Feature Store - Real-time Features      │
└────────┬────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────┐
│             Redis (6379)                        │
│         Feature Cache - Sub-10ms Retrieval      │
└─────────────────────────────────────────────────┘

         Streaming Pipeline (Asynchronous)
┌─────────────────────────────────────────────────┐
│          📬 Kafka + Zookeeper                   │
│         Event Streaming - Rating Updates        │
└────────┬────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────┐
│       🔧 Feature Engineering Service (5002)     │
│         Real-time Feature Computation           │
└─────────────────────────────────────────────────┘

         Monitoring & Tracking
┌─────────────────────────────────────────────────┐
│      🎥 Prometheus (9090) + Grafana (3001)      │
│         Metrics Collection & Dashboards         │
└─────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────┐
│             📦 MLflow (5000)                    │
│         Experiment Tracking & Model Registry    │
└─────────────────────────────────────────────────┘
```

---

## ✨ Features

### Core ML Capabilities
- **Model Serving**: BentoML-powered production inference with automatic batching and optimization
- **Feature Store**: Feast integration with Redis for sub-10ms feature retrieval
- **Real-time Updates**: Kafka streaming pipeline for immediate feature freshness
- **Data Validation**: Great Expectations for robust input validation

### Observability
- **Metrics**: Prometheus scraping for request latency, throughput, and error rates
- **Dashboards**: Pre-configured Grafana dashboards for system health
- **Experiment Tracking**: MLflow for model versioning and performance tracking

### Production Ready
- **Containerized**: All services run in Docker containers
- **Scalable**: Microservices architecture for independent scaling
- **Resilient**: Auto-restart policies and health checks
- **Monitored**: End-to-end observability

---

## 🚀 Quick Start

### Prerequisites

- Docker 20.10+
- Docker Compose 2.0+
- 10GB RAM (minimum)
- 20GB disk space

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/https://github.com/vishwa419/MLOps_Inference.git
   cd MLOps_Inference
   ```

2. **Create required directories**
   ```bash
   mkdir -p data models mlruns
   ```

3. **Start all services**
   ```bash
   docker-compose up -d
   ```

4. **Verify services are running**
   ```bash
   docker-compose ps
   ```

### Access Points

| Service | URL | Purpose |
|---------|-----|---------|
| BentoML API | http://localhost:3000 | Model predictions |
| Validation Service | http://localhost:5001 | Data validation |
| Feature Service | http://localhost:5002 | Feature engineering |
| Feast Service | http://localhost:5003 | Feature retrieval |
| MLflow UI | http://localhost:5000 | Experiment tracking |
| Grafana | http://localhost:3001 | Monitoring dashboards |
| Prometheus | http://localhost:9090 | Metrics database |
| Kafka | localhost:9092 | Event streaming |
| Redis | localhost:6379 | Feature cache |

---

## 🔧 Services

### Infrastructure Services

#### Kafka + Zookeeper
Event streaming backbone for real-time data flow.

```yaml
Ports: 9092 (Kafka), 2181 (Zookeeper)
Resources: ~2-3GB RAM
Latency: 10-100ms
```

**Use Cases:**
- User rating events
- Feature update triggers
- Model prediction logs

#### Redis
High-speed cache for feature storage and retrieval.

```yaml
Port: 6379
Resources: ~500MB RAM
Latency: 5-10ms
Persistence: Append-only file (AOF)
```

### ML Microservices

#### Validation Service (Port 5001)
Validates all incoming data using Great Expectations.

**Validations:**
- Data types and schemas
- Value ranges (e.g., ratings 1-5)
- Required fields presence
- Business logic constraints

#### Feature Engineering Service (Port 5002)
Computes and transforms features in real-time.

**Features:**
- User rating statistics (avg, count)
- Genre preferences
- Recency metrics
- Collaborative filtering features

#### Feast Service (Port 5003)
Feature store for serving pre-computed features.

**Key Benefits:**
- 5-15ms feature retrieval
- Consistent features across training/serving
- Point-in-time correctness
- Feature versioning

#### BentoML Service (Port 3000)
Model serving with production optimizations.

**Capabilities:**
- Automatic batching
- Model versioning
- Built-in metrics
- Concurrent request handling

### Monitoring Services

#### Prometheus (Port 9090)
Time-series metrics collection and storage.

**Metrics Tracked:**
- Request rate and latency
- Error rates
- Resource utilization
- Feature freshness

#### Grafana (Port 3001)
Visualization and alerting dashboards.

**Default Credentials:**
- Username: `admin`
- Password: `admin`

#### MLflow (Port 5000)
Experiment tracking and model registry.

**Features:**
- Model versioning
- Hyperparameter tracking
- Artifact storage
- Model comparison

---

## 📊 Performance

### Real-World Metrics

Based on deployment with **137,734 users** and **26,228 movies**:

#### Synchronous Path (Prediction Request)
```
┌──────────────┬──────────────┐
│ Component    │ Latency      │
├──────────────┼──────────────┤
│ Validation   │ 10-20ms      │
│ Feature Fetch│ 5-15ms       │
│ Model Predict│ 100-400ms    │
├──────────────┼──────────────┤
│ TOTAL        │ 115-435ms ✅ │
└──────────────┴──────────────┘
Target: <500ms
```

#### Asynchronous Path (Feature Update)
```
┌──────────────┬──────────────┐
│ Component    │ Latency      │
├──────────────┼──────────────┤
│ Event Publish│ 5-10ms       │
│ Kafka Queue  │ 10-50ms      │
│ Feature Comp │ 50-200ms     │
│ Redis Update │ 5-10ms       │
├──────────────┼──────────────┤
│ TOTAL        │ 70-270ms ✅  │
└──────────────┴──────────────┘
Target: <1 second
```

#### Resource Usage (All Services)
```
Total RAM:     8-10GB
Total CPU:     6-8 cores
Storage:       ~5GB (models + data)
Network:       Minimal (containerized)
```

---

## 📁 Project Structure

```
mlops-streaming-recommendation/
├── docker-compose.yml          # Service orchestration
├── data/                       # Raw and processed data
├── models/                     # Trained model artifacts
├── features/                   # Feature definitions
├── reports/                    # Validation reports
├── mlruns/                     # MLflow tracking data
├── services/
│   ├── validation/             # Great Expectations service
│   │   ├── Dockerfile
│   │   └── app.py
│   ├── feature_engineering/    # Feature computation
│   │   ├── Dockerfile
│   │   └── app.py
│   ├── feast_service/          # Feature serving
│   │   ├── Dockerfile
│   │   └── feature_store.yaml
│   ├── serving/                # BentoML model serving
│   │   ├── Dockerfile
│   │   └── service.py
│   └── monitoring/
│       ├── prometheus/
│       │   └── prometheus.yml
│       └── grafana/
│           ├── provisioning/
│           └── dashboards/
└── README.md
```

---

## 🔄 Workflow Example

### Making a Prediction

```bash
# Request recommendations for user
curl -X POST http://localhost:3000/predict \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": 100,
    "num_recommendations": 10
  }'
```

**What Happens:**
1. Validation service checks input data (10-20ms)
2. BentoML fetches features from Feast (5-15ms)
3. Feast retrieves cached features from Redis (<10ms)
4. Model generates predictions (100-400ms)
5. Results returned to user

### Updating Features (Streaming)

```python
# User rates a movie
producer.send('user-ratings', {
    'user_id': 100,
    'movie_id': 1234,
    'rating': 5.0,
    'timestamp': datetime.now()
})
```

**What Happens:**
1. Event published to Kafka topic (5-10ms)
2. Feature engineering service consumes event (10-50ms)
3. New features computed (avg_rating, genre_pref, etc.) (50-200ms)
4. Features pushed to Feast → Redis (5-10ms)
5. Next prediction uses fresh features ✅

---

## 🛠️ Configuration

### Environment Variables

Edit `.env` file or modify `docker-compose.yml`:

```env
# Service Names
SERVICE_NAME=mlops-pipeline

# Redis Configuration
REDIS_HOST=redis
REDIS_PORT=6379

# Kafka Configuration
KAFKA_BROKER=kafka:29092

# Feature Store
FEAST_URL=http://feast-service:5003

# Monitoring
PROMETHEUS_URL=http://prometheus:9090
GRAFANA_ADMIN_PASSWORD=admin

# MLflow
MLFLOW_TRACKING_URI=http://mlflow:5000
```

### Resource Limits

Adjust in `docker-compose.yml`:

```yaml
deploy:
  resources:
    limits:
      cpus: '2'
      memory: 2G
    reservations:
      cpus: '1'
      memory: 1G
```

---

## 📈 Monitoring

### Accessing Dashboards

1. **Grafana**: http://localhost:3001
   - Login: `admin` / `admin`
   - Pre-configured dashboards for all services
   - Real-time metrics visualization

2. **Prometheus**: http://localhost:9090
   - Query interface for raw metrics
   - Target health status
   - Alert rule configuration

3. **MLflow**: http://localhost:5000
   - Experiment comparison
   - Model performance tracking
   - Artifact browsing

### Key Metrics to Monitor

- **Latency P50/P95/P99**: Response time percentiles
- **Throughput**: Requests per second
- **Error Rate**: Failed predictions percentage
- **Feature Freshness**: Time since last feature update
- **Resource Usage**: CPU/Memory per service

---

## 🧪 Development

### Running Individual Services

```bash
# Start only infrastructure
docker-compose up -d zookeeper kafka redis

# Start specific service
docker-compose up validation-service

# View logs
docker-compose logs -f bentoml-service

# Restart service
docker-compose restart feast-service
```

### Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Run service locally
cd services/validation
python app.py
```

---

## 🔍 Troubleshooting

### Common Issues

**Services won't start:**
```bash
# Check logs
docker-compose logs

# Verify ports not in use
netstat -tulpn | grep -E '(3000|5000|5001|5002|5003|6379|9092)'

# Restart all services
docker-compose down
docker-compose up -d
```

**Kafka connection issues:**
```bash
# Verify Kafka is running
docker-compose logs kafka

# Check Zookeeper connection
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092
```

**Feature retrieval slow:**
```bash
# Check Redis connection
docker exec -it redis redis-cli ping

# Monitor Redis
docker exec -it redis redis-cli monitor
```

---

## 📚 Further Reading

- [Building MLOps Inference Pipelines](https://medium.com/@kafkafranz495/mlops-inference-pipeline-building-a-streaming-recommendation-system-9e908fb23825)
- [BentoML Documentation](https://docs.bentoml.org/)
- [Feast Feature Store](https://feast.dev/)
- [Great Expectations](https://greatexpectations.io/)
- [Kafka Streams](https://kafka.apache.org/documentation/streams/)

---

## 🤝 Contributing

Contributions welcome! Please follow these steps:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---

## 🙏 Acknowledgments

- Architecture inspired by production MLOps best practices
- Built with modern open-source ML tools
- Community-driven development

---

## 📬 Contact

For questions or feedback:
- Read the [Blog Post](https://medium.com/@kafkafranz495/mlops-inference-pipeline-building-a-streaming-recommendation-system-9e908fb23825)

---

<div align="center">

**⭐ Star this repo if you found it helpful!**

Made with ❤️ for the MLOps community

</div>
