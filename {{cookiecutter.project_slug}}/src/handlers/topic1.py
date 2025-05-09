import logging

from pydantic import ValidationError
from src.handlers.base import BaseHandler, NonRetryableError
from src.models.envelope import MessageEnvelope
from src.models.user import User

logger = logging.getLogger(__name__)

class Topic1Handler(BaseHandler):
    {% if cookiecutter.kafka_library == "confluent-kafka" %}
    def _process_message(self, message: MessageEnvelope) -> bool:
        try:
            logger.info(f"Processing message: {message.to_dict()}")
            # Add your actual message processing logic here
            user = User(**message.body)
            logger.info(f"Found user in message: {user.model_dump_json()}")
            # return True to commit offset
            return True
        except ValidationError as e:
            raise NonRetryableError(e)
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
            user = User(**message.body)
            logger.info(f"Found user in message: {user.model_dump_json()}")
            # return True to commit offset
            return True
        except ValidationError as e:
            raise NonRetryableError(e)
        except Exception as e:
            logger.error(f"Error processing message: {e}", exc_info=True)
            # Depending on your error handling strategy, you might want to:
            # - Return False to not commit and retry the message
            # - Return True to commit and effectively skip the message
            # Consider the implications for your specific use case
            return False
    {% endif %}
