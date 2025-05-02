from .consumer.kafka import KafkaConsumer
from .handlers.topic1 import Topic1Handler

{% if cookiecutter.kafka_library == "confluent-kafka" %}
def main():
    # Create handlers
    topic1_handler = Topic1Handler()
    
    # Create consumer
    consumer = KafkaConsumer()

    # Register handlers
    consumer.register_handler("topic1", topic1_handler)
    
    # Start consumer
    consumer.start()

if __name__ == "__main__":
    main()
{% elif cookiecutter.kafka_library == "aiokafka" %}
async def main():
    # Create handlers
    topic1_handler = Topic1Handler()
    
    # Create consumer
    consumer = KafkaConsumer()

    # Register handlers
    consumer.register_handler("topic1", topic1_handler)
    
    # Start consumer
    await consumer.start()

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
{% endif %}
