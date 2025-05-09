"""
Kafka consumer implementation with dependency injection for handlers.
"""
import datetime
import json
import logging
import random
import signal
from typing import Callable, Dict, Optional

{% if cookiecutter.kafka_library == "confluent-kafka" %}
from time import sleep

from confluent_kafka import Consumer, KafkaError, Message, Producer

{% elif cookiecutter.kafka_library == "aiokafka" %}
import asyncio
from typing import Awaitable

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import TopicPartition

{% endif %}

from src.consumer.config import ConsumerConfig
from src.handlers.base import BaseHandler
from src.models.envelope import MessageEnvelope

"""
{% if cookiecutter.include_prometheus_metrics == "yes" %}
from {{cookiecutter.project_slug}}.consumer.metrics import ConsumerMetrics

{% endif %}
"""

logger = logging.getLogger(__name__)


class KafkaConsumer:
    """
    Kafka consumer that routes messages to topic-specific handlers and
    manages offset commits based on handler success.
    """

    def __init__(self, config: Optional[ConsumerConfig] = None):
        """
        Initialize the Kafka consumer with configuration.
        
        Args:
            config: Consumer configuration
        """
        self.config = config if config is not None else ConsumerConfig() 
        self.handlers: Dict[str, BaseHandler] = {}
        self.running = False
        
        """
        {% if cookiecutter.include_prometheus_metrics == "yes" %}
        self.metrics = ConsumerMetrics()
        {% endif %}
        """
        
        # Set up signal handlers for graceful shutdown
        {% if cookiecutter.kafka_library == "confluent-kafka" %}
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)
        {% elif cookiecutter.kafka_library == "aiokafka" %}
        # Defer signal-handler registration until you’re inside the running loop
        {% endif %}

        {% if cookiecutter.kafka_library == "confluent-kafka" %}
        # Configure Kafka consumer
        self.consumer = Consumer({
            'bootstrap.servers': self.config.bootstrap_servers,
            'group.id': self.config.group_id,
            'auto.offset.reset': self.config.auto_offset_reset,
            'enable.auto.commit': False,
        })

        # Configure retry producer
        self.retry_producer = Producer({
            'bootstrap.servers': self.config.bootstrap_servers,
            'message.send.max.retries': 3,
            'retry.backoff.ms': 500,
            'delivery.timeout.ms': 10000,
        })
        # Configure dead letter queue producer
        self.dlq_producer = Producer({
            'bootstrap.servers': self.config.bootstrap_servers,
            'message.send.max.retries': 3,
            'retry.backoff.ms': 500,
            'delivery.timeout.ms': 10000,
        })
        {% elif cookiecutter.kafka_library == "aiokafka" %}
        # These will be initialized in start() for aiokafka
        self.consumer: Optional[AIOKafkaConsumer] = None
        self.retry_producer: Optional[AIOKafkaProducer] = None
        self.dlq_producer: Optional[AIOKafkaProducer] = None
        {% endif %}

    def register_handler(self, topic: str, handler: BaseHandler) -> None:
        """
        Register a handler for a specific topic.
        
        Args:
            topic: Kafka topic name
            handler: Handler instance for processing messages
        """
        self.handlers[topic] = handler
        logger.info(f"Registered handler {handler.__class__.__name__} for topic {topic}")

    {% if cookiecutter.kafka_library == "confluent-kafka" %}
    def start(self) -> None:
        """Start consuming messages from Kafka."""
        if not self.handlers:
            logger.error("No handlers registered. Exiting.")
            return
        
        # Subscribe to topics
        topics = list(self.handlers.keys())
        self.consumer.subscribe(topics)
        logger.info(f"Subscribed to topics: {', '.join(topics)}")
        
        self.running = True
        
        try:
            while self.running:
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
    
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug(f"Reached end of partition {msg.partition()}")
                    else:
                        logger.error(f"Kafka error: {msg.error()}")
                    continue
                
                topic = msg.topic()
                """
                {% if cookiecutter.include_prometheus_metrics == "yes" %}
                self.metrics.message_received(topic)
                processing_timer = self.metrics.start_processing_timer(topic)
                {% endif %}
                """

                try:
                    handler = self.handlers.get(topic)
                    if not handler:
                        logger.warning(f"No handler registered for topic {topic}")
                        continue
                    
                    # Parse message
                    try:
                        message_data = json.loads(msg.value().decode('utf-8'))
                    except (UnicodeError, json.JSONDecodeError):
                        logger.error(f"Failed to decode message as JSON from topic {topic}")
                        self._send_to_dlq(topic, msg.value(), "Invalid JSON format")
                        self.consumer.commit(msg)
                        """
                        {% if cookiecutter.include_prometheus_metrics == "yes" %}
                        self.metrics.message_failed(topic, "parse_error")
                        {% endif %}
                        """
                        continue
                    
                    # Process message
                    success = handler.handle(message_data, self._get_retry_callback(topic), self._get_dlq_callback(topic, msg.value()))
                    
                    if success:
                        self.consumer.commit(msg)
                        """
                        {% if cookiecutter.include_prometheus_metrics == "yes" %}
                        self.metrics.message_processed(topic)
                        {% endif %}
                        """
                    else:
                        logger.warning(f"Handler returned False for message in topic {topic}")
                        sleep(random.uniform(1, 3)) # avoid hammering both the broker and our logs. 
                        """
                        {% if cookiecutter.include_prometheus_metrics == "yes" %}
                        self.metrics.message_failed(topic, "handler_failure")
                        {% endif %}
                        """
                    
                except Exception as e:
                    logger.exception(f"Error processing message from {topic}: {e}")
                    self._send_to_dlq(topic, msg.value(), str(e))
                    self.consumer.commit(msg)
                    """
                    {% if cookiecutter.include_prometheus_metrics == "yes" %}
                    self.metrics.message_failed(topic, "exception")
                    {% endif %}
                    """
                """
                {% if cookiecutter.include_prometheus_metrics == "yes" %}
                finally:
                    processing_timer.stop_and_record()
                {% endif %}
                """
                
        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        finally:
            logger.info("Closing consumer")
            self.consumer.close()
            # Flush any pending messages and close producers
            logger.info("Flushing and closing producers")
            self.retry_producer.flush()
            self.dlq_producer.flush()

    def _retry_message(self, original_topic: str, failed_message: MessageEnvelope, reason: str) -> None:
        """
        Send a message to the retry queue.
        
        Args:
            original_topic: Original topic the message came from
            failed_message: Envelope for failed message
            reason: Reason for retrying message
        """
        retry_topic = {{cookiecutter.retry_topic}}

        # Extract retry count if present
        header = failed_message.header or {}
        try:
            retry_count = int(header.get("retryCount", 0))
        except ValueError:
            logger.warning(f"Invalid retryCount value: {header.get('retryCount')}, using 0")
            retry_count = 0

        envelope_copy = MessageEnvelope.from_dict(failed_message.to_dict())

        # Increment retry count for next attempt
        envelope_copy.header = header  # ensure not None
        envelope_copy.header["retryCount"] = str(retry_count + 1)

        # Include metadata about original topic
        envelope_copy.header["originalTopic"] = original_topic
        envelope_copy.header["retryReason"] = reason

        try:
            # Produce new message with updated headers
            self.retry_producer.produce(
                retry_topic,
                json.dumps(envelope_copy.to_dict()).encode("utf-8"),
                callback=self._delivery_report
            )
            # allow delivery callback processing without blocking
            self.retry_producer.poll(0)
    
            logger.info(f"Message sent to retry topic {retry_topic}, attempt {retry_count}")
            """
            {% if cookiecutter.include_prometheus_metrics == "yes" %}
            self.metrics.message_retried(topic)
            {% endif %}
            """

        except Exception as e:
            logger.error(f"Failed to send message to retry {retry_topic}: {e}")


    def _send_to_dlq(self, original_topic: str, message: bytes, reason: str) -> None:
        """
        Send a message to the dead letter queue.
        
        Args:
            original_topic: Original topic the message came from
            message: Original message bytes
            reason: Reason for sending to DLQ
        """
        dlq_topic = {{cookiecutter.dlq_topic}}
        
        try:
            # Create a wrapper that includes the original message and metadata
            dlq_message = {
                "original_message": message.decode('utf-8', errors='replace'),
                "error_reason": reason,
                "original_topic": original_topic,
                "timestamp": datetime.datetime.now().isoformat()
            }
            
            self.dlq_producer.produce(
                dlq_topic,
                json.dumps(dlq_message).encode('utf-8'),
                callback=self._delivery_report
            )
            # allow delivery callback processing without blocking
            self.dlq_producer.poll(0)
            
            logger.info(f"Message sent to DLQ topic {dlq_topic}")
            """
            {% if cookiecutter.include_prometheus_metrics == "yes" %}
            self.metrics.message_sent_to_dlq(original_topic)
            {% endif %}
            """
            
        except Exception as e:
            logger.error(f"Failed to send message to DLQ {dlq_topic}: {e}")

    def _delivery_report(self, err: Optional[Exception], msg: Message) -> None:
        """Callback for producer to report delivery success/failure."""
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

    def _get_retry_callback(self, topic: str) -> Callable[[MessageEnvelope, str], None]:
        """
        Return a callback function for handlers to send messages to retry topic.
        
        Args:
            topic: Original topic
        
        Returns:
            Callable function that sends to retry topic
        """
        def retry_message(failed_message: MessageEnvelope, reason: str) -> None:
            self._retry_message(topic, failed_message, reason)
        return retry_message

    def _get_dlq_callback(self, topic: str, original_message: bytes) -> Callable[[str], None]:
        """
        Return a callback function for handlers to send messages to DLQ.
        
        Args:
            topic: Original topic
            original_message: Original message bytes
        
        Returns:
            Callable function that sends to DLQ with given reason
        """
        def send_to_dlq(reason: str) -> None:
            self._send_to_dlq(topic, original_message, reason)
        return send_to_dlq

    def _handle_shutdown(self, signum, frame) -> None:
        """Handle shutdown signals gracefully."""
        logger.info(f"Received signal {signum}, shutting down...")
        self.running = False

    {% elif cookiecutter.kafka_library == "aiokafka" %}
    async def start(self) -> None:
        """Start consuming messages from Kafka."""
        if not self.handlers:
            logger.error("No handlers registered. Exiting.")
            return
        
        # Subscribe to topics
        topics = list(self.handlers.keys())
        
        # Initialize consumer and producer
        self.consumer = AIOKafkaConsumer(
            *topics,
            bootstrap_servers=self.config.bootstrap_servers,
            group_id=self.config.group_id,
            auto_offset_reset=self.config.auto_offset_reset,
            enable_auto_commit=False,
        )
        
        self.retry_producer = AIOKafkaProducer(
            bootstrap_servers=self.config.bootstrap_servers,
        )

        self.dlq_producer = AIOKafkaProducer(
            bootstrap_servers=self.config.bootstrap_servers,
        )
        
        await self.consumer.start()

        # register signals on the running loop
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(self._handle_shutdown(s)))
            except NotImplementedError:
                # Fallback for Windows / non-main thread
                def _sync_shutdown_handler(_sig, _frame):
                    loop = asyncio.get_running_loop()
                    loop.call_soon_threadsafe(asyncio.create_task, self._handle_shutdown(_sig))

                signal.signal(sig, _sync_shutdown_handler)
        
        await self.retry_producer.start()
        await self.dlq_producer.start()
        
        logger.info(f"Subscribed to topics: {', '.join(topics)}")
        
        self.running = True
        
        try:
            while self.running:
                try:
                    async for msg in self.consumer:
                        topic = msg.topic
                        """
                        {% if cookiecutter.include_prometheus_metrics == "yes" %}
                        self.metrics.message_received(topic)
                        processing_timer = self.metrics.start_processing_timer(topic)
                        {% endif %}
                        """

                        try:
                            handler = self.handlers.get(topic)
                            if not handler:
                                logger.warning(f"No handler registered for topic {topic}")
                                continue
                            
                            # Parse message
                            try:
                                message_data = json.loads(msg.value.decode('utf-8'))
                            except (UnicodeError, json.JSONDecodeError):
                                logger.error(f"Failed to decode message as JSON from topic {topic}")
                                await self._send_to_dlq(topic, msg.value, "Invalid JSON format")
                                tp = TopicPartition(msg.topic, msg.partition)
                                await self.consumer.commit({tp: msg.offset + 1})
                                """
                                {% if cookiecutter.include_prometheus_metrics == "yes" %}
                                self.metrics.message_failed(topic, "parse_error")
                                {% endif %}
                                """
                                continue
                             
                            # Process message
                            retry_callback = self._get_retry_callback(topic)
                            dlq_callback = self._get_dlq_callback(topic, msg.value)
                            
                            success = await handler.handle(message_data, retry_callback, dlq_callback)
                            
                            if success:
                                tp = TopicPartition(msg.topic, msg.partition)
                                await self.consumer.commit({tp: msg.offset + 1})
                                """
                                {% if cookiecutter.include_prometheus_metrics == "yes" %}
                                self.metrics.message_processed(topic)
                                {% endif %}
                                """
                            else:
                                logger.warning(f"Handler returned False for message in topic {topic}")
                                await asyncio.sleep(random.uniform(1, 3)) # avoid hammering both the broker and our logs. 
                                """
                                {% if cookiecutter.include_prometheus_metrics == "yes" %}
                                self.metrics.message_failed(topic, "handler_failure")
                                {% endif %}
                                """
                            
                        except Exception as e:
                            logger.exception(f"Error processing message from {topic}: {e}")
                            await self._send_to_dlq(topic, msg.value, str(e))
                            tp = TopicPartition(msg.topic, msg.partition)
                            await self.consumer.commit({tp: msg.offset + 1})
                            """
                            {% if cookiecutter.include_prometheus_metrics == "yes" %}
                            self.metrics.message_failed(topic, "exception")
                            {% endif %}
                            """
                        """
                        {% if cookiecutter.include_prometheus_metrics == "yes" %}
                        finally:
                            processing_timer.stop_and_record()
                        {% endif %}
                        """
                
                except Exception as e:
                    logger.exception(f"Consumer error: {e}")
                    if self.running:
                        await asyncio.sleep(1)
                    
        finally:
            logger.info("Closing consumer and producers")
            await self._shutdown_resources()

    async def _shutdown_resources(self):
        """Shutdown all resources in the correct order."""
        logger.info("Closing producers and consumer")
        if getattr(self, "consumer", None) and self.consumer._closed is False:
            await self.consumer.stop()
        if getattr(self, "retry_producer", None) and self.retry_producer._closed is False:
            await self.retry_producer.stop()
        if getattr(self, "dlq_producer", None) and self.dlq_producer._closed is False:
            await self.dlq_producer.stop()

    async def _retry_message(self, original_topic: str, failed_message: MessageEnvelope, reason: str) -> None:
        """
        Send a message to the retry queue.
        
        Args:
            original_topic: Original topic the message came from
            failed_message: Envelope of failed message
            reason: Reason for retrying message
        """
        retry_topic = {{cookiecutter.retry_topic}}
        
        # Extract retry count if present
        header = failed_message.header or {}
        try:
            retry_count = int(header.get("retryCount", 0))
        except ValueError:
            logger.warning(f"Invalid retryCount value: {header.get('retryCount')}, using 0")
            retry_count = 0

        envelope_copy = MessageEnvelope.from_dict(failed_message.to_dict())

        # Increment retry count for next attempt
        envelope_copy.header = header # ensure not None
        envelope_copy.header["retryCount"] = str(retry_count + 1)

        # Include metadata about original topic
        envelope_copy.header["originalTopic"] = original_topic
        envelope_copy.header["retryReason"] = reason

        try:
            await self.retry_producer.send_and_wait(
                retry_topic,
                json.dumps(envelope_copy.to_dict()).encode('utf-8')
            )
            
            logger.info(f"Message sent to retry topic {retry_topic}, attempt {retry_count}")
            """
            {% if cookiecutter.include_prometheus_metrics == "yes" %}
            self.metrics.message_retried(original_topic)
            {% endif %}
            """

        except Exception as e:
            logger.error(f"Failed to send message to retry {retry_topic}: {e}")

    async def _send_to_dlq(self, original_topic: str, message: bytes, reason: str) -> None:
        """
        Send a message to the dead letter queue.
        
        Args:
            original_topic: Original topic the message came from
            message: Original message bytes
            reason: Reason for sending to DLQ
        """
        dlq_topic = {{cookiecutter.dlq_topic}}
        
        try:
            # Create a wrapper that includes the original message and metadata
            dlq_message = {
                "original_message": message.decode('utf-8', errors='replace'),
                "error_reason": reason,
                "original_topic": original_topic,
                "timestamp": datetime.datetime.now().isoformat()
            }

            await self.dlq_producer.send_and_wait(
                dlq_topic,
                json.dumps(dlq_message).encode('utf-8')
            )
            
            logger.info(f"Message sent to DLQ topic {dlq_topic}")
            """
            {% if cookiecutter.include_prometheus_metrics == "yes" %}
            self.metrics.message_sent_to_dlq(original_topic)
            {% endif %}
            """

        except Exception as e:
            logger.error(f"Failed to send message to DLQ {dlq_topic}: {e}")

    def _get_retry_callback(self, topic: str) -> Callable[[MessageEnvelope, str], Awaitable[None]]:
        """
        Return a callback function for handlers to send messages to retry topic.
        
        Args:
            topic: Original topic
        
        Returns:
            Callable function that sends to retry topic with given reason
        """
        async def retry_message(original_message: MessageEnvelope, reason: str) -> None:
            await self._retry_message(topic, original_message, reason)
        return retry_message

    def _get_dlq_callback(self, topic: str, original_message: bytes) -> Callable[[str], Awaitable[None]]:
        """
        Return a callback function for handlers to send messages to DLQ.
        
        Args:
            topic: Original topic
            original_message: Original message bytes
        
        Returns:
            Callable function that sends to DLQ with given reason
        """
        async def send_to_dlq(reason: str) -> None:
            await self._send_to_dlq(topic, original_message, reason)
        return send_to_dlq

    async def _handle_shutdown(self, signum) -> None:
        """Handle shutdown signals gracefully."""
        logger.info("Received signal %s, shutting down…", signum)
        self.running = False
        await self._shutdown_resources()
    {% endif %}
