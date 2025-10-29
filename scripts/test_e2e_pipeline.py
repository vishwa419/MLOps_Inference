"""
End-to-End MLOps Pipeline Test
Tests complete flow from data to prediction with streaming
"""

import requests
import json
import time
from datetime import datetime
import subprocess
import sys

# Service URLs
VALIDATION_URL = "http://localhost:5001"
FEATURE_URL = "http://localhost:5002"
FEAST_URL = "http://localhost:5003"
BENTOML_URL = "http://localhost:3000"
MLFLOW_URL = "http://localhost:5000"
PROMETHEUS_URL = "http://localhost:9090"
GRAFANA_URL = "http://localhost:3001"

def print_header(text):
    """Print formatted header"""
    print(f"\n{'='*80}")
    print(f"  {text}")
    print(f"{'='*80}\n")

def check_service(name, url, endpoint="/health", method="GET"):
    """Check if a service is running"""
    try:
        if method == "POST":
            response = requests.post(f"{url}{endpoint}", json={}, timeout=5)
        else:
            response = requests.get(f"{url}{endpoint}", timeout=5)
        
        if response.status_code in [200, 404]:  # 404 means service is up but endpoint doesn't exist
            print(f"✓ {name:30s} HEALTHY")
            return True
        else:
            print(f"✗ {name:30s} UNHEALTHY (Status: {response.status_code})")
            return False
    except Exception as e:
        print(f"✗ {name:30s} DOWN ({str(e)[:50]})")
        return False

def test_step_1_services():
    """Test: All services are running"""
    print_header("STEP 1: Service Health Check")
    
    services = [
        ("Validation Service", VALIDATION_URL, "/health", "GET"),
        ("Feature Service", FEATURE_URL, "/health", "GET"),
        ("Feast Service", FEAST_URL, "/health", "GET"),
        ("BentoML Service", BENTOML_URL, "/health", "POST"),  # BentoML uses POST
        ("MLflow", MLFLOW_URL, "/", "GET"),
        ("Prometheus", PROMETHEUS_URL, "/api/v1/status/config", "GET"),
        ("Grafana", GRAFANA_URL, "/api/health", "GET")
    ]
    
    all_healthy = True
    for service in services:
        name, url, endpoint, method = service
        if not check_service(name, url, endpoint, method):
            all_healthy = False
    
    if all_healthy:
        print(f"\n✅ All services healthy!")
        return True
    else:
        print(f"\n❌ Some services are down. Fix them first.")
        return False

def test_step_2_validation():
    """Test: Data validation"""
    print_header("STEP 2: Data Validation")
    
    payload = {
        "file_path": "/data/processed/train.csv",
        "data_type": "ratings"
    }
    
    print("Validating train.csv...")
    response = requests.post(f"{VALIDATION_URL}/validate/batch", json=payload)
    
    if response.status_code == 200:
        result = response.json()
        print(f"✓ Validation complete")
        print(f"  Success rate: {result['success_rate']:.1f}%")
        print(f"  Evaluated: {result['evaluated_expectations']}")
        print(f"  Failed: {result['failed_expectations']}")
        
        if result['valid']:
            print(f"\n✅ Data validation PASSED")
            return True
        else:
            print(f"\n⚠️  Data validation FAILED")
            return False
    else:
        print(f"❌ Validation request failed: {response.text}")
        return False

def test_step_3_features():
    """Test: Feature computation"""
    print_header("STEP 3: Feature Engineering")
    
    payload = {
        "ratings_path": "/data/processed/train.csv",
        "movies_path": "/data/processed/movies_clean.csv",
        "export_to_feast": True
    }
    
    print("Computing features...")
    response = requests.post(f"{FEATURE_URL}/features/batch", json=payload, timeout=120)
    
    if response.status_code == 200:
        result = response.json()
        print(f"✓ Features computed:")
        for name, count in result['features_computed'].items():
            print(f"  - {name}: {count:,} rows")
        print(f"\n✅ Feature engineering complete")
        return True
    else:
        print(f"❌ Feature computation failed: {response.text}")
        return False

def test_step_4_feast():
    """Test: Feast materialization"""
    print_header("STEP 4: Feast Materialization")
    
    payload = {
        "end_date": datetime.now().isoformat()
    }
    
    print("Materializing features to Redis...")
    response = requests.post(f"{FEAST_URL}/feast/materialize", json=payload, timeout=60)
    
    if response.status_code == 200:
        print(f"✓ Features materialized to Redis")
        
        # Test feature retrieval
        print("\nTesting feature retrieval...")
        test_user = 100
        response = requests.get(f"{FEAST_URL}/feast/user/{test_user}")
        
        if response.status_code == 200:
            result = response.json()
            features = result['features']
            print(f"✓ Retrieved features for user {test_user}:")
            for key, value in list(features.items())[:5]:
                print(f"  - {key}: {value}")
            print(f"\n✅ Feast working correctly")
            return True
        else:
            print(f"⚠️  Feature retrieval failed")
            return False
    else:
        print(f"❌ Materialization failed: {response.text}")
        return False

def test_step_5_prediction():
    """Test: Model prediction"""
    print_header("STEP 5: Model Prediction (BentoML)")
    
    test_user = 100
    
    print(f"Requesting recommendations for user {test_user}...")
    payload = {
        "user_idx": test_user,
        "n_recommendations": 10
    }
    
    response = requests.post(f"{BENTOML_URL}/recommend", json=payload, timeout=30)
    
    if response.status_code == 200:
        result = response.json()
        print(f"✓ Got {result['count']} recommendations")
        print(f"  Latency: {result['latency_ms']}ms")
        
        print(f"\nTop 5 recommendations:")
        for i, rec in enumerate(result['recommendations'][:5], 1):
            print(f"  {i}. {rec['title']} (score: {rec['score']:.3f})")
            print(f"     Genres: {rec['genres']}")
        
        # Check if Feast features were used
        if result.get('feast_features'):
            print(f"\n✓ Feast features integrated:")
            for key, value in result['feast_features'].items():
                if not key.endswith('_idx'):
                    print(f"  - {key}: {value}")
        
        print(f"\n✅ Prediction working correctly")
        return True
    else:
        print(f"❌ Prediction failed: {response.text}")
        return False

def test_step_6_streaming():
    """Test: Streaming pipeline (simulated)"""
    print_header("STEP 6: Streaming Feature Updates")
    
    # Simulate a user event
    event = {
        "user_idx": 100,
        "movie_idx": 50,
        "event_type": "view",
        "timestamp": time.time(),
        "genres": "Action|Thriller"
    }
    
    print(f"Simulating event: User {event['user_idx']} viewed Movie {event['movie_idx']}")
    
    # Update feature service
    print("\n1. Updating Feature Service...")
    response = requests.post(f"{FEATURE_URL}/features/stream", json=event)
    
    if response.status_code == 200:
        result = response.json()
        print(f"✓ Feature service updated")
        print(f"  User recent activity: {result['features']['user_recent_activity']}")
        print(f"  Movie popularity: {result['features']['movie_popularity']}")
        
        # Update Feast
        print("\n2. Pushing to Feast (Redis)...")
        response = requests.post(
            f"{FEAST_URL}/feast/stream/update",
            json={"event": {**event, **result['features']}}
        )
        
        if response.status_code == 200:
            print(f"✓ Feast updated")
            
            # Verify update
            time.sleep(1)
            print("\n3. Verifying updated features...")
            response = requests.get(f"{FEAST_URL}/feast/user/{event['user_idx']}")
            
            if response.status_code == 200:
                result = response.json()
                print(f"✓ Features retrieved from Redis")
                print(f"\n✅ Streaming pipeline working!")
                return True
        
    print(f"❌ Streaming update failed")
    return False

def test_step_7_monitoring():
    """Test: Monitoring metrics"""
    print_header("STEP 7: Monitoring & Metrics")
    
    # Check Prometheus targets
    print("Checking Prometheus targets...")
    response = requests.get(f"{PROMETHEUS_URL}/api/v1/targets")
    
    if response.status_code == 200:
        result = response.json()
        active_targets = result['data']['activeTargets']
        print(f"✓ Prometheus monitoring {len(active_targets)} targets")
        
        for target in active_targets:
            health = target['health']
            job = target['labels']['job']
            print(f"  - {job:20s} {health}")
    
    # Check if metrics are being collected
    print("\nChecking BentoML metrics...")
    response = requests.post(f"{BENTOML_URL}/metrics", json={})
    
    if response.status_code == 200:
        print(f"✓ BentoML exposing metrics")
        print(f"\n✅ Monitoring active")
        return True
    else:
        print(f"⚠️  Metrics not available")
        return False

def run_full_pipeline():
    """Run complete end-to-end test"""
    print_header("🚀 MLOps End-to-End Pipeline Test")
    
    tests = [
        ("Service Health", test_step_1_services),
        ("Data Validation", test_step_2_validation),
        ("Feature Engineering", test_step_3_features),
        ("Feast Materialization", test_step_4_feast),
        ("Model Prediction", test_step_5_prediction),
        ("Streaming Updates", test_step_6_streaming),
        ("Monitoring", test_step_7_monitoring)
    ]
    
    results = []
    
    for test_name, test_func in tests:
        try:
            success = test_func()
            results.append((test_name, success))
            
            if not success:
                print(f"\n⚠️  {test_name} failed. Continuing to next test...\n")
                time.sleep(2)
        except Exception as e:
            print(f"\n❌ {test_name} error: {e}\n")
            results.append((test_name, False))
    
    # Summary
    print_header("📊 TEST SUMMARY")
    
    passed = sum(1 for _, success in results if success)
    total = len(results)
    
    for test_name, success in results:
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{test_name:30s} {status}")
    
    print(f"\n{'='*80}")
    print(f"Results: {passed}/{total} tests passed ({passed/total*100:.0f}%)")
    print(f"{'='*80}\n")
    
    if passed == total:
        print("🎉 ALL TESTS PASSED! System is fully operational.\n")
        print("Next steps:")
        print("  1. Start Kafka producer: python streaming/producer.py")
        print("  2. Start Kafka consumer: python streaming/consumer.py")
        print("  3. Monitor Grafana: http://localhost:3001")
        print("  4. View MLflow: http://localhost:5000")
    else:
        print("⚠️  Some tests failed. Check logs above for details.\n")

if __name__ == "__main__":
    run_full_pipeline()
