from confluent_kafka import Producer
import json
import time

def delivery_report(err, msg):
    """
    Callback function to handle message delivery reports.
    Prints an error if the message failed to be delivered.
    """
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def create_kafka_producer(bootstrap_servers, config=None):
    """
    Create and return a Kafka producer.
    
    :param bootstrap_servers: Comma-separated list of Kafka broker addresses
    :param config: Additional configuration dictionary (optional)
    :return: Confluent Kafka Producer instance
    """
    # Default configuration
    default_config = {
        'bootstrap.servers': bootstrap_servers,
        'client.id': 'python-producer',
        'acks': 'all'  # Wait for all in-sync replicas to acknowledge
    }
    
    # Update with any additional config
    if config:
        default_config.update(config)
    
    return Producer(default_config)

def produce_messages(producer, topic, messages):
    """
    Produce messages to a Kafka topic.
    
    :param producer: Confluent Kafka Producer instance
    :param topic: Kafka topic name
    :param messages: List of messages to produce
    """
    try:
        for message in messages:
            # Serialize message to JSON if it's not already a string
            if not isinstance(message, str):
                message = json.dumps(message)
            
            # Produce message to the topic
            producer.produce(
                topic, 
                value=message.encode('utf-8'), 
                callback=delivery_report
            )
        
        # Wait for any outstanding messages to be delivered
        producer.flush()
    
    except Exception as e:
        print(f'Error producing messages: {e}')

def main():
    # Kafka broker configuration
    bootstrap_servers = 'localhost:9092'  # Replace with your Kafka broker address
    topic = 'example_topic1'
    
    # Create producer
    producer = create_kafka_producer(bootstrap_servers)
    
    # Example messages
    messages = [
        {'id': 1, 'message': 'Hello Kafka', 'timestamp': time.time()},
        {'id': 2, 'message': 'Confluent Kafka Producer', 'timestamp': time.time()},
        'Plain text message'
    ]
    
    # Produce messages
    produce_messages(producer, topic, messages)

if __name__ == '__main__':
    main()