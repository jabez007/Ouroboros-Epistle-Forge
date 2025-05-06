import logging

{% if cookiecutter.include_schema_validation == "yes" %}
from typing import Any, Dict

{% endif %}

from src.handlers.base import BaseHandler
from src.models.envelope import MessageEnvelope

logger = logging.getLogger(__name__)

class Topic1Handler(BaseHandler):
    {% if cookiecutter.kafka_library == "confluent-kafka" %}
    def _process_message(self, message: MessageEnvelope) -> bool:
        try:
            logger.info(f"Processing message: {message.to_dict()}")
            # Add your actual message processing logic here
            
            # return True to commit offset
            return True
        except Exception as e:
            logger.error(f"Error processing message: {e}", exc_info=True)
            # Depending on your error handling strategy, you might want to:
            # - Return False to not commit and retry the message
            # - Return True to commit and effectively skip the message
            # Consider the implications for your specific use case
            return False
    {% elif cookiecutter.kafka_library == "aiokafka" %}
    async def _process_message(self, message: MessageEnvelope) -> bool:
        try:
            logger.info(f"Processing message: {message.to_dict()}")
            # Add your actual message processing logic here
            
            # return True to commit offset
            return True
        except Exception as e:
            logger.error(f"Error processing message: {e}", exc_info=True)
            # Depending on your error handling strategy, you might want to:
            # - Return False to not commit and retry the message
            # - Return True to commit and effectively skip the message
            # Consider the implications for your specific use case
            return False
    {% endif %}

    {% if cookiecutter.include_schema_validation == "yes" %}
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
