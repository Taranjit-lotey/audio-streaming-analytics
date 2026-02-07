"""
Audio Event Generator - Simulates realistic music streaming events for testing.

This producer generates events mimicking real user behavior:
- Play/pause/skip/complete actions
- Realistic listening patterns (skips happen early, completes happen late)
- Multiple device types
- Configurable throughput

Usage:
    # Stream to Kinesis (production)
    python event_generator.py --events-per-second 100 --duration-minutes 5

    # Stream to console for local testing
    python event_generator.py --local --events-per-second 10 --duration-minutes 1

    # Redirect to file
    python event_generator.py --local --events-per-second 100 > events.jsonl

Author: Taran
"""

import json
import boto3
import random
import uuid
import argparse
import time
import sys
from datetime import datetime
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor

# Initialize Kinesis client (only when needed)
kinesis: Optional[Any] = None

# Configuration
STREAM_NAME = 'audio-events-stream'
BATCH_SIZE = 100  # Kinesis PutRecords limit


@dataclass
class Track:
    """Represents a music track."""
    track_id: str
    title: str
    artist: str
    duration_ms: int


# Sample track catalog (in production, this would come from a database)
TRACK_CATALOG: List[Track] = [
    Track("t001", "Bohemian Rhapsody", "Queen", 354000),
    Track("t002", "Hotel California", "Eagles", 391000),
    Track("t003", "Stairway to Heaven", "Led Zeppelin", 482000),
    Track("t004", "Imagine", "John Lennon", 183000),
    Track("t005", "Smells Like Teen Spirit", "Nirvana", 301000),
    Track("t006", "Billie Jean", "Michael Jackson", 294000),
    Track("t007", "Like a Rolling Stone", "Bob Dylan", 369000),
    Track("t008", "Hey Jude", "The Beatles", 431000),
    Track("t009", "Purple Rain", "Prince", 520000),
    Track("t010", "One", "U2", 276000),
    Track("t011", "Wonderwall", "Oasis", 258000),
    Track("t012", "Sweet Child O' Mine", "Guns N' Roses", 356000),
    Track("t013", "Lose Yourself", "Eminem", 326000),
    Track("t014", "Rolling in the Deep", "Adele", 228000),
    Track("t015", "Shape of You", "Ed Sheeran", 234000),
]

# User behavior simulation parameters
DEVICE_TYPES = ['MOBILE', 'DESKTOP', 'SMART_SPEAKER', 'SMART_TV', 'WEARABLE']
DEVICE_WEIGHTS = [0.45, 0.25, 0.15, 0.10, 0.05]  # Mobile dominant

ACTION_PROBABILITIES = {
    'PLAY': 0.40,
    'PAUSE': 0.15,
    'SKIP': 0.25,
    'COMPLETE': 0.20
}


def generate_user_id() -> str:
    """Generate a realistic user ID from a pool of active users."""
    # Simulate 10,000 active users with some being more active than others
    # Use Zipf distribution to model power users
    user_number = int(random.paretovariate(1.5)) % 10000
    return f"user_{user_number:05d}"


def generate_event() -> Dict[str, Any]:
    """
    Generate a single audio event with realistic patterns.
    
    Returns:
        Dict representing an audio streaming event
    """
    # Select random track
    track = random.choice(TRACK_CATALOG)
    
    # Select action based on probabilities
    action = random.choices(
        list(ACTION_PROBABILITIES.keys()),
        weights=list(ACTION_PROBABILITIES.values())
    )[0]
    
    # Calculate realistic listen duration based on action
    if action == 'SKIP':
        # Skips typically happen early (first 30 seconds)
        listen_duration_ms = random.randint(5000, 30000)
    elif action == 'COMPLETE':
        # Completes are near full duration (90-100%)
        listen_duration_ms = int(track.duration_ms * random.uniform(0.90, 1.0))
    elif action == 'PAUSE':
        # Pauses happen anywhere in the track
        listen_duration_ms = random.randint(10000, track.duration_ms - 10000)
    else:  # PLAY
        listen_duration_ms = 0  # Just started
    
    # Select device with weighted probability
    device_type = random.choices(DEVICE_TYPES, weights=DEVICE_WEIGHTS)[0]
    
    event = {
        'user_id': generate_user_id(),
        'session_id': f"sess_{uuid.uuid4().hex[:12]}",
        'track_id': track.track_id,
        'track_title': track.title,
        'artist': track.artist,
        'action': action,
        'device_type': device_type,
        'listen_duration_ms': listen_duration_ms,
        'event_timestamp': datetime.utcnow().isoformat() + 'Z'
    }
    
    return event


def send_batch_to_kinesis(events: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Send a batch of events to Kinesis Data Streams.

    Uses user_id as partition key to ensure events for the same user
    go to the same shard (important for ordering).

    Args:
        events: List of event dictionaries

    Returns:
        Kinesis PutRecords response
    """
    global kinesis
    if kinesis is None:
        kinesis = boto3.client('kinesis')

    records = [
        {
            'Data': json.dumps(event).encode('utf-8'),
            'PartitionKey': event['user_id']  # Same user → same shard
        }
        for event in events
    ]

    response = kinesis.put_records(
        StreamName=STREAM_NAME,
        Records=records
    )

    return response


def send_batch_to_console(events: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Send a batch of events to console (stdout) for local testing.

    Prints each event as a JSON line to stdout, which can be:
    - Viewed in the terminal
    - Redirected to a file: python event_generator.py --local > events.jsonl
    - Piped to another process: python event_generator.py --local | jq .

    Args:
        events: List of event dictionaries

    Returns:
        Simulated response matching Kinesis format
    """
    for event in events:
        # Print each event as JSON line (JSONL format)
        print(json.dumps(event), file=sys.stdout, flush=True)

    # Return simulated successful response
    return {
        'FailedRecordCount': 0,
        'Records': [{'SequenceNumber': str(i), 'ShardId': 'local'} for i in range(len(events))]
    }


def run_generator(events_per_second: int, duration_minutes: int, local_mode: bool = False) -> None:
    """
    Run the event generator at specified throughput.

    Args:
        events_per_second: Target event rate
        duration_minutes: How long to run
        local_mode: If True, print to console instead of sending to Kinesis
    """
    total_events = events_per_second * duration_minutes * 60
    events_sent = 0
    failed_records = 0
    start_time = time.time()

    # Select appropriate send function
    send_function = send_batch_to_console if local_mode else send_batch_to_kinesis
    destination = "console (stdout)" if local_mode else f"Kinesis stream: {STREAM_NAME}"

    print(f"Starting event generation:", file=sys.stderr)
    print(f"  Target rate: {events_per_second} events/second", file=sys.stderr)
    print(f"  Duration: {duration_minutes} minutes", file=sys.stderr)
    print(f"  Total events: {total_events:,}", file=sys.stderr)
    print(f"  Destination: {destination}", file=sys.stderr)
    print("-" * 50, file=sys.stderr)

    # Calculate batch timing
    batches_per_second = max(1, events_per_second // BATCH_SIZE)
    sleep_time = 1.0 / batches_per_second
    batch_size = events_per_second // batches_per_second

    try:
        while events_sent < total_events:
            batch_start = time.time()

            # Generate batch of events
            events = [generate_event() for _ in range(batch_size)]

            # Send to appropriate destination
            response = send_function(events)

            # Track failures
            failed_in_batch = response.get('FailedRecordCount', 0)
            failed_records += failed_in_batch
            events_sent += len(events)

            # Progress update every 1000 events
            if events_sent % 1000 == 0:
                elapsed = time.time() - start_time
                rate = events_sent / elapsed
                print(f"  Sent: {events_sent:,} | Rate: {rate:.1f}/s | Failed: {failed_records}", file=sys.stderr)

            # Maintain target rate
            batch_elapsed = time.time() - batch_start
            if batch_elapsed < sleep_time:
                time.sleep(sleep_time - batch_elapsed)

    except KeyboardInterrupt:
        print("\nInterrupted by user", file=sys.stderr)

    # Final stats
    elapsed = time.time() - start_time
    print("-" * 50, file=sys.stderr)
    print(f"Generation complete:", file=sys.stderr)
    print(f"  Events sent: {events_sent:,}", file=sys.stderr)
    print(f"  Failed records: {failed_records}", file=sys.stderr)
    print(f"  Elapsed time: {elapsed:.1f}s", file=sys.stderr)
    print(f"  Actual rate: {events_sent/elapsed:.1f} events/second", file=sys.stderr)


def generate_sample_file(num_events: int, output_path: str) -> None:
    """
    Generate sample events to a JSON file for testing.
    
    Args:
        num_events: Number of events to generate
        output_path: Path to output file
    """
    events = [generate_event() for _ in range(num_events)]
    
    with open(output_path, 'w') as f:
        json.dump(events, f, indent=2)
    
    print(f"Generated {num_events} sample events to {output_path}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generate audio streaming events')
    parser.add_argument('--events-per-second', type=int, default=100,
                        help='Target events per second (default: 100)')
    parser.add_argument('--duration-minutes', type=int, default=5,
                        help='Duration in minutes (default: 5)')
    parser.add_argument('--local', action='store_true',
                        help='Stream to console (stdout) instead of Kinesis for local testing')
    parser.add_argument('--sample-file', type=str,
                        help='Generate sample file instead of streaming')
    parser.add_argument('--sample-count', type=int, default=100,
                        help='Number of events for sample file (default: 100)')

    args = parser.

    if args.sample_file:
        generate_sample_file(args.sample_count, args.sample_file)
    else:
        run_generator(args.events_per_second, args.duration_minutes, local_mode=args.local)
