import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
import pandas as pd

# Load user and movie data to generate realistic events
print("Loading data for event generation...")
train = pd.read_csv('data/processed/train.csv')
movies = pd.read_csv('data/processed/movies_clean.csv')

user_ids = train['user_idx'].unique().tolist()
movie_ids = train['movie_idx'].unique().tolist()

print(f"Loaded {len(user_ids)} users and {len(movie_ids)} movies")

# Initialize Kafka producer
print("Connecting to Kafka...")
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

event_types = ['view', 'like', 'rate', 'search', 'click']
event_weights = [0.5, 0.2, 0.15, 0.1, 0.05]  # View is most common

print("\nStarting event stream (Ctrl+C to stop)...")
print(f"Target: ~10 events/second")
print("-" * 60)

event_count = 0
start_time = time.time()

try:
    while True:
        # Generate random event
        event = {
            'event_id': f"evt_{int(time.time() * 1000)}_{random.randint(1000, 9999)}",
            'user_idx': random.choice(user_ids),
            'movie_idx': random.choice(movie_ids),
            'event_type': random.choices(event_types, weights=event_weights)[0],
            'timestamp': datetime.utcnow().isoformat(),
            'session_id': f"session_{random.randint(1, 10000)}"
        }
        
        # Add rating if event_type is 'rate'
        if event['event_type'] == 'rate':
            event['rating'] = random.choice([0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0])
        
        # Send to Kafka
        producer.send('movie_events', value=event)
        
        event_count += 1
        
        # Print progress every 100 events
        if event_count % 100 == 0:
            elapsed = time.time() - start_time
            rate = event_count / elapsed
            print(f"Sent {event_count} events | Rate: {rate:.1f} events/sec | Last: {event['event_type']}")
        
        # Sleep to control rate (~10 events/sec)
        time.sleep(0.1)

except KeyboardInterrupt:
    print(f"\n\nStopping producer...")
    print(f"Total events sent: {event_count}")
    print(f"Duration: {time.time() - start_time:.1f} seconds")
    producer.close()
