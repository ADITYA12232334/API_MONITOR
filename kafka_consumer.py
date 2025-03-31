from confluent_kafka import Consumer, KafkaError, KafkaException
import sys
import logging
import json

# Configure logging correctly (fixing the formatting error)
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_consumer(config):
    """Create a Kafka consumer instance with the given configuration."""
    consumer = Consumer(config)
    return consumer

def consume_messages(consumer, topics):
    """Consume and process messages from the specified Kafka topics."""
    try:
        # Subscribe to topics
        consumer.subscribe(topics)
        logger.info(f"Subscribed to topics: {', '.join(topics)}")
        
        while True:
            # Poll for messages with a timeout of 1.0 second
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
                
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event - not an error
                    logger.info(f"Reached end of partition {msg.partition()}")
                elif msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    # Topic doesn't exist - log warning and continue
                    logger.warning(f"Topic not available: {msg.topic()}")
                    # You might want to sleep here before retrying
                    import time
                    time.sleep(5)
                else:
                    # Log other errors but don't raise exceptions
                    logger.error(f"Kafka error: {msg.error()}")
            else:
                # Process the message
                try:
                    # Try to parse as JSON
                    value = json.loads(msg.value().decode('utf-8'))
                    logger.info(f"Topic: {msg.topic()}, Partition: {msg.partition()}, "
                               f"Offset: {msg.offset()}, Key: {msg.key()}, Value: {value}")
                except json.JSONDecodeError:
                    # If not JSON, treat as string
                    logger.info(f"Topic: {msg.topic()}, Partition: {msg.partition()}, "
                               f"Offset: {msg.offset()}, Key: {msg.key()}, "
                               f"Value: {msg.value().decode('utf-8')}")
                    
    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
    finally:
        # Close consumer to commit final offsets
        consumer.close()
        logger.info("Consumer closed")

if __name__ == "__main__":
    # Consumer configuration
    config = {
        'bootstrap.servers': 'localhost:9092',  # Kafka broker(s)
        'group.id': 'my-consumer-group',        # Consumer group ID
        'auto.offset.reset': 'earliest',        # Start consuming from earliest message
        'enable.auto.commit': True,             # Automatic commit of offsets
        'auto.commit.interval.ms': 5000,        # Commit every 5 seconds
        'max.poll.interval.ms': 300000,         # 5 minutes in milliseconds
        'session.timeout.ms': 30000,            # 30 seconds in milliseconds
    }
    
    # Topics to consume from - make sure this topic exists on your Kafka broker
    topics = ['example_topic']
    
    # Create and start the consumer
    consumer = create_consumer(config)
    
    # Consume messages
    consume_messages(consumer, topics)