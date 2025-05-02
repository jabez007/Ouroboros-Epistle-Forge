import logging

from ..models.envelope import MessageEnvelope
from .base import BaseHandler

logger = logging.getLogger(__name__)

class Topic1Handler(BaseHandler):
    async def _process_message(self, message: MessageEnvelope) -> bool:
       logger.info(message.to_dict())
       # return True to commit offset
       return True
