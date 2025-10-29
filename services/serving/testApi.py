import requests
import json
import time
from typing import Optional

BASE_URL = "http://localhost:3000"

class Colors:
    """ANSI color codes for pretty terminal output"""
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    END = '\033[0m'
    BOLD = '\033[1m'

def print_header(text):
    """Print a colored header"""
    print(f"\n{Colors.BOLD}{Colors.HEADER}{'='*80}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.HEADER}{text.center(80)}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.HEADER}{'='*80}{Colors.END}\n")

def print_section(text):
    """Print a colored section"""
    print(f"\n{Colors.BOLD}{Colors.CYAN}{text}{Colors.END}")
    print(f"{Colors.CYAN}{'-'*80}{Colors.END}")

def print_success(text):
    """Print success message"""
    print(f"{Colors.GREEN}✓ {text}{Colors.END}")

def print_error(text):
    """Print error message"""
    print(f"{Colors.RED}✗ {text}{Colors.END}")

def print_info(text):
    """Print info message"""
    print(f"{Colors.BLUE}ℹ {text}{Colors.END}")

def test_health():
    """Test health endpoint"""
    print_section("Test 1: Health Check Endpoint")
    
    try:
        start_time = time.time()
        response = requests.get(f"{BASE_URL}/health")
        elapsed = (time.time() - start_time) * 1000
        
        if response.status_code == 200:
            data = response.json()
            print_success(f"Health check passed ({elapsed:.2f}ms)")
            print_info(f"Status: {data['status']}")
            print_info(f"Model loaded: {data['model_loaded']}")
            print_info(f"Total users: {data['total_users']:,}")
            print_info(f"Total movies: {data['total_movies']:,}")
            return True
        else:
            print_error(f"Health check failed with status {response.status_code}")
            print(response.text)
            return False
    except Exception as e:
        print_error(f"Health check failed: {str(e)}")
        return False

def test_recommend(user_idx=100, n_recommendations=10):
    """Test recommend endpoint"""
    print_section(f"Test 2: Basic Recommendations (User {user_idx})")
    
    params = {
        "user_idx": user_idx,
        "n_recommendations": n_recommendations,
        "exclude_rated": True
    }
    
    try:
        start_time = time.time()
        response = requests.post(
            f"{BASE_URL}/recommend",
            params=params
        )
        elapsed = (time.time() - start_time) * 1000
        
        if response.status_code == 200:
            data = response.json()
            print_success(f"Recommendations generated ({elapsed:.2f}ms)")
            print_info(f"User: {data['user_idx']}")
            print_info(f"Recommendations returned: {data['count']}")
            
            print(f"\n{Colors.BOLD}Top {min(5, len(data['recommendations']))} Recommendations:{Colors.END}")
            for i, rec in enumerate(data['recommendations'][:5], 1):
                print(f"\n  {Colors.YELLOW}{i}. {rec['title']}{Colors.END}")
                print(f"     Score: {rec['score']:.4f}")
                print(f"     Genres: {rec['genres']}")
            
            return True
        else:
            print_error(f"Request failed with status {response.status_code}")
            print(response.text)
            return False
    except Exception as e:
        print_error(f"Request failed: {str(e)}")
        return False

def test_batch_recommend(user_idx=250, n_recommendations=5):
    """Test batch_recommend endpoint with history"""
    print_section(f"Test 3: Batch Recommendations with History (User {user_idx})")
    
    payload = {
        "user_idx": user_idx,
        "n_recommendations": n_recommendations,
        "exclude_rated": True
    }
    
    try:
        start_time = time.time()
        response = requests.post(
            f"{BASE_URL}/batch_recommend",
            json=payload,
            headers={"Content-Type": "application/json"}
        )
        elapsed = (time.time() - start_time) * 1000
        
        if response.status_code == 200:
            data = response.json()
            print_success(f"Batch recommendations generated ({elapsed:.2f}ms)")
            
            # Display user history
            print(f"\n{Colors.BOLD}User's Top Rated Movies:{Colors.END}")
            for i, movie in enumerate(data['user_history'][:5], 1):
                rating_stars = '★' * int(movie['rating']) + '☆' * (5 - int(movie['rating']))
                print(f"\n  {Colors.GREEN}{i}. {movie['title']}{Colors.END}")
                print(f"     Rating: {rating_stars} ({movie['rating']:.1f}/5.0)")
                print(f"     Genres: {movie['genres']}")
            
            # Display recommendations
            print(f"\n{Colors.BOLD}Personalized Recommendations:{Colors.END}")
            for i, rec in enumerate(data['recommendations'], 1):
                print(f"\n  {Colors.YELLOW}{i}. {rec['title']}{Colors.END}")
                print(f"     Score: {rec['score']:.4f}")
                print(f"     Genres: {rec['genres']}")
            
            return True
        else:
            print_error(f"Request failed with status {response.status_code}")
            print(response.text)
            return False
    except Exception as e:
        print_error(f"Request failed: {str(e)}")
        return False

def test_different_users():
    """Test multiple different users"""
    print_section("Test 4: Multiple Users")
    
    test_users = [50, 100, 500, 1000]
    results = []
    
    for user_idx in test_users:
        payload = {
            "user_idx": user_idx,
            "n_recommendations": 3,
            "exclude_rated": True
        }
        
        try:
            start_time = time.time()
            response = requests.post(
                f"{BASE_URL}/recommend",
                json=payload,
                headers={"Content-Type": "application/json"}
            )
            elapsed = (time.time() - start_time) * 1000
            
            if response.status_code == 200:
                data = response.json()
                results.append({
                    'user_idx': user_idx,
                    'success': True,
                    'elapsed': elapsed,
                    'count': data['count']
                })
                print_success(f"User {user_idx}: {data['count']} recommendations in {elapsed:.2f}ms")
            else:
                results.append({'user_idx': user_idx, 'success': False})
                print_error(f"User {user_idx}: Failed")
        except Exception as e:
            results.append({'user_idx': user_idx, 'success': False})
            print_error(f"User {user_idx}: {str(e)}")
    
    # Summary
    success_count = sum(1 for r in results if r.get('success'))
    avg_time = sum(r.get('elapsed', 0) for r in results if r.get('success')) / max(success_count, 1)
    
    print(f"\n{Colors.BOLD}Summary:{Colors.END}")
    print_info(f"Successful requests: {success_count}/{len(test_users)}")
    print_info(f"Average response time: {avg_time:.2f}ms")
    
    return success_count == len(test_users)

def test_edge_cases():
    """Test error handling and edge cases"""
    print_section("Test 5: Error Handling & Edge Cases")
    
    test_cases = [
        {
            "name": "Invalid user (too high)",
            "payload": {"user_idx": 999999, "n_recommendations": 10},
            "expect_error": True
        },
        {
            "name": "Negative user index",
            "payload": {"user_idx": -1, "n_recommendations": 10},
            "expect_error": True
        },
        {
            "name": "Zero recommendations",
            "payload": {"user_idx": 100, "n_recommendations": 0},
            "expect_error": True
        },
        {
            "name": "Too many recommendations",
            "payload": {"user_idx": 100, "n_recommendations": 1000},
            "expect_error": True
        },
        {
            "name": "Valid edge case (1 recommendation)",
            "payload": {"user_idx": 100, "n_recommendations": 1},
            "expect_error": False
        }
    ]
    
    passed = 0
    for test in test_cases:
        try:
            response = requests.post(
                f"{BASE_URL}/recommend",
                json=test["payload"],
                headers={"Content-Type": "application/json"}
            )
            
            if test["expect_error"]:
                if response.status_code != 200:
                    print_success(f"{test['name']}: Correctly rejected")
                    passed += 1
                else:
                    print_error(f"{test['name']}: Should have failed but didn't")
            else:
                if response.status_code == 200:
                    print_success(f"{test['name']}: Passed")
                    passed += 1
                else:
                    print_error(f"{test['name']}: Should have passed but failed")
        except Exception as e:
            print_error(f"{test['name']}: Exception - {str(e)}")
    
    print(f"\n{Colors.BOLD}Edge cases passed: {passed}/{len(test_cases)}{Colors.END}")
    return passed == len(test_cases)

def test_performance():
    """Test API performance with multiple requests"""
    print_section("Test 6: Performance Test")
    
    num_requests = 10
    user_idx = 100
    
    print_info(f"Sending {num_requests} concurrent-style requests...")
    
    times = []
    for i in range(num_requests):
        payload = {
            "user_idx": user_idx,
            "n_recommendations": 10,
            "exclude_rated": True
        }
        
        try:
            start_time = time.time()
            response = requests.post(
                f"{BASE_URL}/recommend",
                json=payload,
                headers={"Content-Type": "application/json"}
            )
            elapsed = (time.time() - start_time) * 1000
            
            if response.status_code == 200:
                times.append(elapsed)
        except Exception as e:
            print_error(f"Request {i+1} failed: {str(e)}")
    
    if times:
        avg_time = sum(times) / len(times)
        min_time = min(times)
        max_time = max(times)
        
        print(f"\n{Colors.BOLD}Performance Stats:{Colors.END}")
        print_info(f"Successful requests: {len(times)}/{num_requests}")
        print_info(f"Average response time: {avg_time:.2f}ms")
        print_info(f"Min response time: {min_time:.2f}ms")
        print_info(f"Max response time: {max_time:.2f}ms")
        
        if avg_time < 200:
            print_success("Performance is excellent! (< 200ms average)")
        elif avg_time < 500:
            print_success("Performance is good (< 500ms average)")
        else:
            print_error("Performance needs improvement (> 500ms average)")
        
        return True
    else:
        print_error("No successful requests")
        return False

def main():
    """Run all tests"""
    print_header("BentoML Movie Recommender API Test Suite")
    
    print_info(f"Testing API at: {BASE_URL}")
    print_info("Make sure the BentoML server is running:")
    print_info("  $ bentoml serve service:MovieRecommenderService --reload\n")
    
    time.sleep(1)
    
    # Run all tests
    tests = [
        ("Health Check", test_health),
        ("Basic Recommendations", lambda: test_recommend(100, 10)),
        ("Batch Recommendations", lambda: test_batch_recommend(250, 5)),
        ("Multiple Users", test_different_users),
        ("Error Handling", test_edge_cases),
        ("Performance", test_performance)
    ]
    
    results = []
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print_error(f"Test '{test_name}' crashed: {str(e)}")
            results.append((test_name, False))
        time.sleep(0.5)
    
    # Final summary
    print_header("Test Summary")
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = f"{Colors.GREEN}PASSED{Colors.END}" if result else f"{Colors.RED}FAILED{Colors.END}"
        print(f"  {test_name:.<60} {status}")
    
    print(f"\n{Colors.BOLD}Overall: {passed}/{total} tests passed{Colors.END}")
    
    if passed == total:
        print_success("\n🎉 All tests passed! Your API is working correctly.")
    else:
        print_error(f"\n⚠️  {total - passed} test(s) failed. Please check the output above.")
    
    print(f"\n{Colors.BOLD}Next Steps:{Colors.END}")
    print("  1. Check the API docs: http://localhost:3000/docs")
    print("  2. Try the interactive UI: http://localhost:3000")
    print("  3. Build the Bento: bentoml build")
    print("  4. Containerize: bentoml containerize movie_recommender:latest\n")

if __name__ == "__main__":
    main()
