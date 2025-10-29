import json
import time
from kafka import KafkaConsumer
from collections import defaultdict, deque
from datetime import datetime

print("Connecting to Kafka...")
consumer = KafkaConsumer(
    'movie_events',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='latest',  # Start from latest messages
    group_id='movie-event-processor'
)

print("Connected! Waiting for events...\n")
print("-" * 80)

# Track statistics
event_count = 0
event_types_count = defaultdict(int)
recent_events = deque(maxlen=10)  # Store last 10 events
start_time = time.time()

# Track user activity (for feature updates)
user_activity = defaultdict(int)  # user_idx -> event count
movie_popularity = defaultdict(int)  # movie_idx -> view count

try:
    for message in consumer:
        event = message.value
        event_count += 1
        
        # Update statistics
        event_types_count[event['event_type']] += 1
        user_activity[event['user_idx']] += 1
        
        if event['event_type'] in ['view', 'click']:
            movie_popularity[event['movie_idx']] += 1
        
        # Store recent event
        recent_events.append(event)
        
        # Print summary every 100 events
        if event_count % 100 == 0:
            elapsed = time.time() - start_time
            rate = event_count / elapsed
            
            print(f"\n{'='*80}")
            print(f"Events processed: {event_count} | Rate: {rate:.1f} events/sec")
            print(f"Event types: {dict(event_types_count)}")
            print(f"Active users (last window): {len(user_activity)}")
            print(f"Popular movies (last window): {len(movie_popularity)}")
            print(f"\nLast 3 events:")
            for evt in list(recent_events)[-3:]:
                print(f"  - User {evt['user_idx']} -> {evt['event_type']} -> Movie {evt['movie_idx']}")
            print(f"{'='*80}\n")
        
        # Simulate feature update (in production, this would write to Redis/Feast)
        if event_count % 1000 == 0:
            print(f"[FEATURE UPDATE] Would update online features for {len(user_activity)} users")
            # Reset activity window
            user_activity.clear()
            movie_popularity.clear()

except KeyboardInterrupt:
    print(f"\n\nStopping consumer...")
    print(f"Total events processed: {event_count}")
    print(f"Duration: {time.time() - start_time:.1f} seconds")
    consumer.close()
