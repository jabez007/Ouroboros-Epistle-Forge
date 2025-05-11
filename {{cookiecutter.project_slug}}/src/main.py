import logging
import signal

{% if cookiecutter.kafka_library == "aiokafka" %}
import asyncio

{% endif %}

from src.consumer.kafka import KafkaConsumer
from src.handlers.topic1 import Topic1Handler

SHUTDOWN_SIGNALS = [signal.SIGTERM, signal.SIGINT]

{% if cookiecutter.kafka_library == "confluent-kafka" %}
def main():
    try:
        # Create handlers
        topic1_handler = Topic1Handler()
    
        # Create consumer
        consumer = KafkaConsumer()

        # Register handlers
        consumer.register_handler("topic1", topic1_handler)

        # Register shutdown signals
        for sig in SHUTDOWN_SIGNALS:
            signal.signal(sig, consumer._handle_shutdown)

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

        # register signals on the running loop
        main_loop = asyncio.get_running_loop()
        for sig in SHUTDOWN_SIGNALS:
            try:
                main_loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(consumer._handle_shutdown(s)))
            except NotImplementedError:
                # Fallback for Windows / non-main thread
                def _sync_shutdown_handler(_sig, _frame, _loop=main_loop):
                    _loop.call_soon_threadsafe(
                        lambda: asyncio.create_task(consumer._handle_shutdown(_sig))
                    )

                signal.signal(sig, _sync_shutdown_handler)

        # Start consumer
        await consumer.start()

    except Exception as e:
        logging.error(f"Error in main function: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
{% endif%}
