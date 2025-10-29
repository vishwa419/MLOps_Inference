#!/usr/bin/env python3
"""
Load testing script for the Movie Recommender API

Generates realistic traffic patterns to test the monitoring dashboard
"""

import requests
import time
import random
import threading
from datetime import datetime
from collections import defaultdict

BASE_URL = "http://localhost:3000"

# Statistics
stats = defaultdict(int)
stats_lock = threading.Lock()

def make_request(user_idx, n_recommendations=10):
    """Make a single recommendation request"""
    try:
        start = time.time()
        response = requests.post(
            f"{BASE_URL}/recommend",
            json={
                "user_idx": user_idx,
                "n_recommendations": n_recommendations,
                "exclude_rated": True
            },
            timeout=5
        )
        elapsed = time.time() - start
        
        with stats_lock:
            if response.status_code == 200:
                stats['success'] += 1
                stats['total_latency'] += elapsed
            else:
                stats['errors'] += 1
        
        return response.status_code == 200
    
    except Exception as e:
        with stats_lock:
            stats['exceptions'] += 1
        return False

def worker(worker_id, duration, requests_per_second):
    """Worker thread that generates requests"""
    print(f"Worker {worker_id} started")
    
    end_time = time.time() + duration
    delay = 1.0 / requests_per_second
    
    request_count = 0
    
    while time.time() < end_time:
        # Random user between 0 and 10000
        user_idx = random.randint(0, 10000)
        
        # Random number of recommendations
        n_recs = random.choice([5, 10, 15, 20])
        
        make_request(user_idx, n_recs)
        request_count += 1
        
        # Sleep to maintain rate
        time.sleep(delay)
    
    print(f"Worker {worker_id} finished ({request_count} requests)")

def print_stats():
    """Print current statistics"""
    with stats_lock:
        total = stats['success'] + stats['errors'] + stats['exceptions']
        if total == 0:
            return
        
        success_rate = (stats['success'] / total) * 100
        avg_latency = stats['total_latency'] / max(stats['success'], 1)
        
        print(f"\n{'='*60}")
        print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*60}")
        print(f"Total requests:    {total:,}")
        print(f"Successful:        {stats['success']:,} ({success_rate:.1f}%)")
        print(f"Errors:            {stats['errors']:,}")
        print(f"Exceptions:        {stats['exceptions']:,}")
        print(f"Avg latency:       {avg_latency*1000:.2f}ms")
        print(f"{'='*60}\n")

def run_load_test(
    duration_seconds=300,
    num_workers=5,
    requests_per_second=2
):
    """
    Run load test
    
    Args:
        duration_seconds: How long to run the test
        num_workers: Number of concurrent workers
        requests_per_second: Requests per second per worker
    """
    print("="*60)
    print("Movie Recommender Load Test")
    print("="*60)
    print(f"Duration: {duration_seconds}s")
    print(f"Workers: {num_workers}")
    print(f"Requests/sec per worker: {requests_per_second}")
    print(f"Total target throughput: {num_workers * requests_per_second} req/s")
    print("="*60)
    print("")
    
    # Start worker threads
    threads = []
    for i in range(num_workers):
        t = threading.Thread(
            target=worker,
            args=(i, duration_seconds, requests_per_second)
        )
        t.start()
        threads.append(t)
    
    # Print stats every 10 seconds
    start_time = time.time()
    while time.time() - start_time < duration_seconds:
        time.sleep(10)
        print_stats()
    
    # Wait for all threads to finish
    for t in threads:
        t.join()
    
    # Final stats
    print("\n" + "="*60)
    print("FINAL RESULTS")
    print_stats()

def run_spike_test(baseline_rps=2, spike_rps=20, spike_duration=30):
    """
    Run a spike test: baseline load → spike → baseline
    
    Useful for testing how the system handles sudden load increases
    """
    print("="*60)
    print("Spike Test")
    print("="*60)
    print(f"Baseline: {baseline_rps} req/s for 60s")
    print(f"Spike: {spike_rps} req/s for {spike_duration}s")
    print(f"Baseline: {baseline_rps} req/s for 60s")
    print("="*60)
    print("")
    
    # Phase 1: Baseline
    print("Phase 1: Baseline load...")
    run_load_test(duration_seconds=60, num_workers=1, requests_per_second=baseline_rps)
    
    # Phase 2: Spike
    print("\nPhase 2: SPIKE!")
    run_load_test(duration_seconds=spike_duration, num_workers=5, requests_per_second=spike_rps)
    
    # Phase 3: Back to baseline
    print("\nPhase 3: Back to baseline...")
    run_load_test(duration_seconds=60, num_workers=1, requests_per_second=baseline_rps)

def run_realistic_pattern():
    """
    Simulate realistic traffic pattern with varying load
    """
    print("="*60)
    print("Realistic Traffic Pattern")
    print("="*60)
    print("Simulating daily traffic with peaks and valleys")
    print("="*60)
    print("")
    
    patterns = [
        ("Low traffic (morning)", 1, 60),
        ("Rising traffic", 3, 60),
        ("Peak traffic (lunch)", 8, 60),
        ("Normal traffic", 4, 60),
        ("Peak traffic (evening)", 10, 60),
        ("Declining traffic", 3, 60),
        ("Low traffic (night)", 1, 60),
    ]
    
    for phase_name, rps, duration in patterns:
        print(f"\n{'='*60}")
        print(f"Phase: {phase_name}")
        print(f"{'='*60}")
        run_load_test(
            duration_seconds=duration,
            num_workers=max(1, rps // 2),
            requests_per_second=2
        )

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python load_test.py basic         # Basic load test (5 min)")
        print("  python load_test.py quick         # Quick test (1 min)")
        print("  python load_test.py spike         # Spike test")
        print("  python load_test.py realistic     # Realistic pattern (7 min)")
        print("")
        print("Example: python load_test.py quick")
        sys.exit(1)
    
    test_type = sys.argv[1].lower()
    
    if test_type == "basic":
        run_load_test(duration_seconds=300, num_workers=5, requests_per_second=2)
    elif test_type == "quick":
        run_load_test(duration_seconds=60, num_workers=3, requests_per_second=2)
    elif test_type == "spike":
        run_spike_test()
    elif test_type == "realistic":
        run_realistic_pattern()
    else:
        print(f"Unknown test type: {test_type}")
        sys.exit(1)
