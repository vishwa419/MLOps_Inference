"""
Test script for Validation Service API
"""

import requests
import json
import time

BASE_URL = "http://localhost:5001"

def test_health():
    """Test health endpoint"""
    print("="*60)
    print("Test 1: Health Check")
    print("="*60)
    
    response = requests.get(f"{BASE_URL}/health")
    print(f"Status: {response.status_code}")
    print(f"Response: {json.dumps(response.json(), indent=2)}")
    print()

def test_batch_validation_ratings():
    """Test batch validation for ratings"""
    print("="*60)
    print("Test 2: Batch Validation - Ratings")
    print("="*60)
    
    payload = {
        "file_path": "/data/processed/train.csv",
        "data_type": "ratings"
    }
    
    response = requests.post(
        f"{BASE_URL}/validate/batch",
        json=payload
    )
    
    print(f"Status: {response.status_code}")
    
    if response.status_code == 200:
        data = response.json()
        print(f"\n{'✓ PASSED' if data['valid'] else '✗ FAILED'}")
        print(f"Evaluated Expectations: {data['evaluated_expectations']}")
        print(f"Successful: {data['successful_expectations']}")
        print(f"Failed: {data['failed_expectations']}")
        print(f"Success Rate: {data['success_rate']:.1f}%")
        
        if data['report_url']:
            print(f"\nReport URL: {BASE_URL}{data['report_url']}")
        
        if data['failures']:
            print(f"\nFailures:")
            for failure in data['failures']:
                print(f"  - {failure['expectation_type']} on {failure['column']}")
    else:
        print(f"Error: {response.text}")
    
    print()

def test_batch_validation_movies():
    """Test batch validation for movies"""
    print("="*60)
    print("Test 3: Batch Validation - Movies")
    print("="*60)
    
    payload = {
        "file_path": "/data/processed/movies_clean.csv",
        "data_type": "movies"
    }
    
    response = requests.post(
        f"{BASE_URL}/validate/batch",
        json=payload
    )
    
    print(f"Status: {response.status_code}")
    
    if response.status_code == 200:
        data = response.json()
        print(f"\n{'✓ PASSED' if data['valid'] else '✗ FAILED'}")
        print(f"Evaluated Expectations: {data['evaluated_expectations']}")
        print(f"Successful: {data['successful_expectations']}")
        print(f"Failed: {data['failed_expectations']}")
        print(f"Success Rate: {data['success_rate']:.1f}%")
        
        if data['report_url']:
            print(f"\nReport URL: {BASE_URL}{data['report_url']}")
    else:
        print(f"Error: {response.text}")
    
    print()

def test_stream_validation_valid():
    """Test stream validation with valid event"""
    print("="*60)
    print("Test 4: Stream Validation - Valid Event")
    print("="*60)
    
    payload = {
        "user_idx": 100,
        "movie_idx": 50,
        "event_type": "rating",
        "timestamp": time.time(),
        "rating": 4.5
    }
    
    response = requests.post(
        f"{BASE_URL}/validate/stream",
        json=payload
    )
    
    print(f"Status: {response.status_code}")
    
    if response.status_code == 200:
        data = response.json()
        print(f"\n{'✓ VALID' if data['valid'] else '✗ INVALID'}")
        if data['errors']:
            print(f"Errors: {data['errors']}")
    else:
        print(f"Error: {response.text}")
    
    print()

def test_stream_validation_invalid():
    """Test stream validation with invalid event"""
    print("="*60)
    print("Test 5: Stream Validation - Invalid Event")
    print("="*60)
    
    payload = {
        "user_idx": 100,
        "movie_idx": 50,
        "event_type": "rating",
        "timestamp": time.time(),
        "rating": 6.5  # Invalid: out of range
    }
    
    response = requests.post(
        f"{BASE_URL}/validate/stream",
        json=payload
    )
    
    print(f"Status: {response.status_code}")
    
    if response.status_code == 200:
        data = response.json()
        print(f"\n{'✓ VALID' if data['valid'] else '✗ INVALID'}")
        if data['errors']:
            print(f"Errors:")
            for error in data['errors']:
                print(f"  - {error}")
    else:
        print(f"Error: {response.text}")
    
    print()

def test_list_reports():
    """Test listing reports"""
    print("="*60)
    print("Test 6: List Validation Reports")
    print("="*60)
    
    response = requests.get(f"{BASE_URL}/reports")
    
    print(f"Status: {response.status_code}")
    
    if response.status_code == 200:
        data = response.json()
        print(f"\nFound {len(data['reports'])} reports:")
        for report in data['reports'][:5]:  # Show first 5
            print(f"  - {report['filename']}")
            print(f"    URL: {BASE_URL}{report['url']}")
            print(f"    Created: {report['created']}")
    else:
        print(f"Error: {response.text}")
    
    print()

def main():
    print("\n")
    print("*"*60)
    print("Validation Service API Tests")
    print("*"*60)
    print()
    
    try:
        # Run tests
        test_health()
        test_batch_validation_ratings()
        test_batch_validation_movies()
        test_stream_validation_valid()
        test_stream_validation_invalid()
        test_list_reports()
        
        print("*"*60)
        print("✓ All tests completed!")
        print("*"*60)
        print()
        print("Next steps:")
        print("  1. View reports in browser: http://localhost:5001/reports")
        print("  2. Check API docs: http://localhost:5001/docs")
        print("  3. Integrate with training pipeline")
        print()
        
    except requests.exceptions.ConnectionError:
        print("\n✗ ERROR: Cannot connect to validation service")
        print("  Make sure the service is running:")
        print("    docker-compose up -d")
        print()
    except Exception as e:
        print(f"\n✗ ERROR: {str(e)}")
        print()

if __name__ == "__main__":
    main()
