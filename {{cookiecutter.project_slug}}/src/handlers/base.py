"""
Base handler interface for processing Kafka messages.
"""
import abc
import logging
from typing import Any, Callable, Dict

{% if cookiecutter.kafka_library == "aiokafka" %}
from typing import Awaitable

{% endif %}

{% if cookiecutter.include_schema_validation == "yes" %}
import jsonschema

{% endif %}

from src.models.envelope import MessageEnvelope

logger = logging.getLogger(__name__)


class BaseHandler(abc.ABC):
    """
    Base handler interface that all topic-specific handlers must implement.
    """
    
    def __init__(self, max_retries: int = {{ cookiecutter.default_retries }}):
        """
        Initialize handler.
        
        Args:
            max_retries: Maximum number of retry attempts before sending to DLQ
        """
        self.max_retries = max_retries
    
    {% if cookiecutter.kafka_library == "confluent-kafka" %}
    def handle(self, message_data: Dict[str, Any], retry_message: Callable[[MessageEnvelope, str], None], send_to_dlq: Callable[[str], None]) -> bool:
        """
        Process a message from Kafka.
        
        Args:
            message_data: The JSON-decoded message data
            retry_message: Callback function to retry processing a message
            send_to_dlq: Callback function to send message to DLQ
            
        Returns:
            bool: True if message was processed successfully, False otherwise
        """
        try:
            # Parse the envelope structure
            envelope = MessageEnvelope.from_dict(message_data)
            
            # Validate the message format
            if not self._validate_message(envelope):
                send_to_dlq("Invalid message format")
                return True
            
            # Extract retry count if present
            try:
                retry_count = int(envelope.header.get("retryCount", 0))
            except (ValueError, TypeError):
                logger.warning(f"Invalid retryCount value: {envelope.header.get('retryCount')}, using 0")
                retry_count = 0
            
            try:
                # Process the message
                return self._process_message(envelope)
                
            except Exception as e:
                logger.exception(f"Error processing message: {e}")
                
                # Check if we should retry
                if retry_count < self.max_retries:
                    logger.info(f"Retrying message, attempt {retry_count + 1} of {self.max_retries}")
                    retry_message(envelope, repr(e))
                    return True
                else:
                    logger.warning(f"Max retries ({self.max_retries}) reached, sending to DLQ")
                    send_to_dlq(f"Max retries reached: {str(e)}")
                    return True
                    
        except Exception as e:
            logger.exception(f"Error parsing message envelope: {e}")
            send_to_dlq(f"Error parsing message envelope: {str(e)}")
            return True
    {% elif cookiecutter.kafka_library == "aiokafka" %}
    async def handle(self, message_data: Dict[str, Any], retry_message: Callable[[MessageEnvelope, str], Awaitable[None]], send_to_dlq: Callable[[str], Awaitable[None]]) -> bool:
        """
        Process a message from Kafka.
        
        Args:
            message_data: The JSON-decoded message data
            retry_message: Callback function to retry processing a message
            send_to_dlq: Callback function to send message to DLQ
            
        Returns:
            bool: True if message was processed successfully, False otherwise
        """
        try:
            # Parse the envelope structure
            envelope = MessageEnvelope.from_dict(message_data)
            
            # Validate the message format
            if not self._validate_message(envelope):
                await send_to_dlq("Invalid message format")
                return True
            
            # Extract retry count if present
            try:
                retry_count = int(envelope.header.get("retryCount", 0))
            except (ValueError, TypeError):
                logger.warning(f"Invalid retryCount value: {envelope.header.get('retryCount')}, using 0")
                retry_count = 0

            try:
                # Process the message
                return await self._process_message(envelope)
                
            except Exception as e:
                logger.exception(f"Error processing message: {e}")
                
                # Check if we should retry
                if retry_count < self.max_retries:
                    logger.info(f"Retrying message, attempt {retry_count + 1} of {self.max_retries}")
                    await retry_message(envelope, repr(e))
                    return True
                else:
                    logger.warning(f"Max retries ({self.max_retries}) reached, sending to DLQ")
                    await send_to_dlq(f"Max retries reached: {str(e)}")
                    return True
                    
        except Exception as e:
            logger.exception(f"Error parsing message envelope: {e}")
            await send_to_dlq(f"Error parsing message envelope: {str(e)}")
            return True
    {% endif %}
    
    def _validate_message(self, envelope: MessageEnvelope) -> bool:
        """
        Validate message format.
        
        Args:
            envelope: Message envelope to validate
            
        Returns:
            bool: True if message is valid, False otherwise
        """
        # Validate required header fields
        missing = [f for f in MessageEnvelope._REQUIRED_HEADERS if f not in envelope.header]
        if missing:
            logger.error(f"Missing required header fields: {', '.join(missing)}")
            return False
        
        {% if cookiecutter.include_schema_validation == "yes" %}
        # Apply schema validation if enabled
        try:
            self._validate_schema(envelope)
        except jsonschema.exceptions.ValidationError as e:
            logger.error(f"Schema validation failed: {e}")
            return False
        {% endif %}
        
        return True
    
    {% if cookiecutter.include_schema_validation == "yes" %}
    def _validate_schema(self, envelope: MessageEnvelope) -> None:
        """
        Validate message against JSON schema.
        
        Args:
            envelope: Message envelope to validate
            
        Raises:
            jsonschema.exceptions.ValidationError: If validation fails
        """
        # Header schema validation
        from {{cookiecutter.project_slug}}.schemas import get_header_schema
        jsonschema.validate(envelope.header, get_header_schema())
        
        # Body schema validation (implemented by subclasses)
        self._validate_body_schema(envelope.body)
    
    @abc.abstractmethod
    def _validate_body_schema(self, body: Dict[str, Any]) -> None:
        """
        Validate message body against schema.
        
        Args:
            body: Message body to validate
            
        Raises:
            jsonschema.exceptions.ValidationError: If validation fails
        """
        pass
    {% endif %}
    
    {% if cookiecutter.kafka_library == "confluent-kafka" %}
    @abc.abstractmethod
    def _process_message(self, envelope: MessageEnvelope) -> bool:
        """
        Process the message. Must be implemented by subclasses.
        
        Args:
            envelope: Message envelope containing header and body
        """
        pass
    {% elif cookiecutter.kafka_library == "aiokafka" %}
    @abc.abstractmethod
    async def _process_message(self, envelope: MessageEnvelope) -> bool:
        """
        Process the message. Must be implemented by subclasses.
        
        Args:
            envelope: Message envelope containing header and body
        """
        pass
    {% endif %}
