"""
Lambda function to process Kinesis audio events and maintain real-time session state in DynamoDB.

This function is triggered by Kinesis Data Streams and:
1. Decodes and parses incoming audio events
2. Updates user session state in DynamoDB with sub-10ms latency
3. Implements TTL for automatic data lifecycle management

Author: Taran
"""

import json
import base64
import boto3
import logging
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, Any, List

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize DynamoDB resource
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('audio-user-sessions')

# TTL configuration (24 hours from event time)
TTL_HOURS = 24


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Process Kinesis records and update DynamoDB session state.
    
    Args:
        event: Kinesis event containing batch of records
        context: Lambda context object
        
    Returns:
        Dict with processing results
    """
    records_processed = 0
    records_failed = 0
    
    for record in event.get('Records', []):
        try:
            # Decode Kinesis record
            payload = base64.b64decode(record['kinesis']['data'])
            audio_event = json.loads(payload)
            
            # Process the event
            process_audio_event(audio_event)
            records_processed += 1
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON: {e}")
            records_failed += 1
        except Exception as e:
            logger.error(f"Failed to process record: {e}")
            records_failed += 1
    
    result = {
        'statusCode': 200,
        'body': {
            'records_processed': records_processed,
            'records_failed': records_failed
        }
    }
    
    logger.info(f"Processing complete: {result}")
    return result


def process_audio_event(event: Dict[str, Any]) -> None:
    """
    Process a single audio event and update DynamoDB.
    
    Event schema:
    {
        "user_id": "user_123",
        "session_id": "sess_abc",
        "track_id": "track_456",
        "track_title": "Song Name",
        "artist": "Artist Name",
        "action": "PLAY|PAUSE|SKIP|COMPLETE",
        "device_type": "MOBILE|DESKTOP|SMART_SPEAKER",
        "listen_duration_ms": 45000,
        "event_timestamp": "2024-01-15T10:30:00Z"
    }
    """
    # Validate required fields
    required_fields = ['user_id', 'session_id', 'track_id', 'action', 'event_timestamp']
    for field in required_fields:
        if field not in event:
            raise ValueError(f"Missing required field: {field}")
    
    # Parse timestamp
    event_time = datetime.fromisoformat(event['event_timestamp'].replace('Z', '+00:00'))
    
    # Calculate TTL (24 hours from event time)
    ttl_time = event_time + timedelta(hours=TTL_HOURS)
    ttl_epoch = int(ttl_time.timestamp())
    
    # Build DynamoDB item
    item = {
        'user_id': event['user_id'],                          # Partition Key
        'timestamp': event['event_timestamp'],                 # Sort Key
        'session_id': event['session_id'],
        'track_id': event['track_id'],
        'track_title': event.get('track_title', 'Unknown'),
        'artist': event.get('artist', 'Unknown'),
        'action': event['action'],
        'device_type': event.get('device_type', 'UNKNOWN'),
        'listen_duration_ms': Decimal(str(event.get('listen_duration_ms', 0))),
        'ttl': ttl_epoch
    }
    
    # Write to DynamoDB
    table.put_item(Item=item)
    
    logger.debug(f"Wrote event for user {event['user_id']}: {event['action']} on {event['track_id']}")
