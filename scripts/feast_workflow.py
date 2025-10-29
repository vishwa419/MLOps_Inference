"""
Complete Feast Workflow
Demonstrates the full feature engineering -> Feast -> Serving pipeline
"""

import requests
import json
import time
from datetime import datetime

BASE_URL_FEATURE = "http://localhost:5002"
BASE_URL_FEAST = "http://localhost:5003"

def check_services():
    """Check if services are running"""
    print("Checking services...")
    
    try:
        response = requests.get(f"{BASE_URL_FEATURE}/health")
        print(f"✓ Feature Service: {response.json()['status']}")
    except:
        print("✗ Feature Service: Not running")
        return False
    
    try:
        response = requests.get(f"{BASE_URL_FEAST}/health")
        print(f"✓ Feast Service: {response.json()['status']}")
    except:
        print("✗ Feast Service: Not running")
        return False
    
    return True

def compute_batch_features():
    """Step 1: Compute batch features"""
    print("\n" + "="*60)
    print("STEP 1: Computing Batch Features")
    print("="*60)
    
    payload = {
        "ratings_path": "/data/processed/train.csv",
        "movies_path": "/data/processed/movies_clean.csv",
        "export_to_feast": True
    }
    
    print("Sending request to feature service...")
    response = requests.post(
        f"{BASE_URL_FEATURE}/features/batch",
        json=payload
    )
    
    if response.status_code == 200:
        result = response.json()
        print("✓ Features computed successfully!")
        print(f"  - User features: {result['features_computed'].get('user_features', 0)} rows")
        print(f"  - Movie features: {result['features_computed'].get('movie_features', 0)} rows")
        print(f"  - User-movie features: {result['features_computed'].get('user_movie_features', 0)} rows")
        return True
    else:
        print(f"✗ Error: {response.text}")
        return False

def materialize_to_redis():
    """Step 2: Materialize features to Redis (online store)"""
    print("\n" + "="*60)
    print("STEP 2: Materializing Features to Redis")
    print("="*60)
    
    payload = {
        "end_date": datetime.now().isoformat()
    }
    
    print("Materializing batch features to Redis...")
    response = requests.post(
        f"{BASE_URL_FEAST}/feast/materialize",
        json=payload
    )
    
    if response.status_code == 200:
        print("✓ Batch features materialized to Redis!")
        return True
    else:
        print(f"✗ Error: {response.text}")
        return False

def test_online_features():
    """Step 3: Test getting online features"""
    print("\n" + "="*60)
    print("STEP 3: Testing Online Feature Retrieval")
    print("="*60)
    
    # Test user features
    test_user_id = 100
    print(f"\nGetting features for user {test_user_id}...")
    
    response = requests.get(f"{BASE_URL_FEAST}/feast/user/{test_user_id}")
    
    if response.status_code == 200:
        result = response.json()
        features = result['features']
        print(f"✓ User {test_user_id} features:")
        for key, value in features.items():
            if not key.endswith('_idx'):
                print(f"  - {key}: {value}")
    else:
        print(f"✗ Error: {response.text}")
    
    # Test movie features
    test_movie_id = 50
    print(f"\nGetting features for movie {test_movie_id}...")
    
    response = requests.get(f"{BASE_URL_FEAST}/feast/movie/{test_movie_id}")
    
    if response.status_code == 200:
        result = response.json()
        features = result['features']
        print(f"✓ Movie {test_movie_id} features:")
        for key, value in features.items():
            if not key.endswith('_idx'):
                print(f"  - {key}: {value}")
    else:
        print(f"✗ Error: {response.text}")

def simulate_stream_event():
    """Step 4: Simulate streaming feature update"""
    print("\n" + "="*60)
    print("STEP 4: Simulating Stream Feature Update")
    print("="*60)
    
    event = {
        "user_idx": 100,
        "movie_idx": 50,
        "event_type": "view",
        "timestamp": time.time(),
        "genres": "Action|Thriller"
    }
    
    print(f"Simulating event: User {event['user_idx']} viewed Movie {event['movie_idx']}")
    
    # Update in feature service
    response = requests.post(
        f"{BASE_URL_FEATURE}/features/stream",
        json=event
    )
    
    if response.status_code == 200:
        result = response.json()
        print("✓ Stream features updated in feature service:")
        print(f"  - User recent activity: {result['features']['user_recent_activity']}")
        print(f"  - Movie popularity: {result['features']['movie_popularity']}")
    
    # Update in Feast (Redis)
    response = requests.post(
        f"{BASE_URL_FEAST}/feast/stream/update",
        json={"event": event}
    )
    
    if response.status_code == 200:
        print("✓ Stream features pushed to Redis via Feast")
    
    # Retrieve updated features
    print("\nRetrieving updated features from Redis...")
    time.sleep(1)  # Give Redis a moment
    
    response = requests.get(f"{BASE_URL_FEAST}/feast/user/{event['user_idx']}")
    if response.status_code == 200:
        result = response.json()
        print("✓ Updated user features:")
        print(f"  - Recent activity: {result['features'].get('user_recent_activity', 'N/A')}")

def get_feast_stats():
    """Step 5: Get Feast statistics"""
    print("\n" + "="*60)
    print("STEP 5: Feast Feature Store Statistics")
    print("="*60)
    
    response = requests.get(f"{BASE_URL_FEAST}/feast/stats")
    
    if response.status_code == 200:
        result = response.json()
        stats = result['stats']
        
        print("\nFeature Views:")
        for fv in stats['feature_views']:
            print(f"  - {fv['name']}: {fv['num_features']} features (TTL: {fv['ttl_hours']}h)")
        
        print("\nEntities:")
        for entity in stats['entities']:
            print(f"  - {entity['name']}: {entity['join_keys']}")

def main():
    """Run complete workflow"""
    print("\n" + "="*60)
    print("FEAST COMPLETE WORKFLOW")
    print("="*60)
    
    # Check services
    if not check_services():
        print("\n✗ Services not running. Start with: docker-compose up -d")
        return
    
    # Run workflow
    if compute_batch_features():
        time.sleep(2)
        
        if materialize_to_redis():
            time.sleep(2)
            test_online_features()
            time.sleep(1)
            simulate_stream_event()
            time.sleep(1)
            get_feast_stats()
    
    print("\n" + "="*60)
    print("WORKFLOW COMPLETE")
    print("="*60)
    print("\nYour feature store is now ready for:")
    print("  1. Training: Get historical features from Feast")
    print("  2. Serving: Get online features from Redis via Feast")
    print("  3. Streaming: Update features in real-time")

if __name__ == "__main__":
    main()
