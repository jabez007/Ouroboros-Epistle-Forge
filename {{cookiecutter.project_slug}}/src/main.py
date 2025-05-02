import logging

from .consumer.kafka import KafkaConsumer
from .handlers.topic1 import Topic1Handler

{% if cookiecutter.kafka_library == "confluent-kafka" %}
def main():
    try:
        # Create handlers
        topic1_handler = Topic1Handler()
    
        # Create consumer
        consumer = KafkaConsumer()

        # Register handlers
        consumer.register_handler("topic1", topic1_handler)
    
        # Start consumer
        consumer.start()
    except Exception as e:
        logging.error(f"Error in main function: {e}")
        raise

if __name__ == "__main__":
    main()
{% elif cookiecutter.kafka_library == "aiokafka" %}
async def main():
    try:
        # Create handlers
        topic1_handler = Topic1Handler()
    
        # Create consumer
        consumer = KafkaConsumer()

        # Register handlers
        consumer.register_handler("topic1", topic1_handler)
    
        # Start consumer
        await consumer.start()

    except Exception as e:
        logging.error(f"Error in main function: {e}")
        raise

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
{% endif%}
