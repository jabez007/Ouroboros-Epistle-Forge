"""
Message envelope model that follows a SOAP-like structure with header and body.
"""

from dataclasses import dataclass
from typing import Any, Dict


@dataclass
class MessageEnvelope:
    """
    Message envelope with header and body sections.

    The header contains metadata about the message that is consistent
    across all topics. The body contains topic-specific data.
    """

    header: Dict[str, Any]
    body: Dict[str, Any]

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MessageEnvelope":
        """
        Create an envelope from a dictionary.

        Args:
            data: Dictionary containing header and body

        Returns:
            MessageEnvelope: Instantiated envelope

        Raises:
            ValueError: If the dictionary is missing required fields
        """
        if "header" not in data or "body" not in data:
            raise ValueError("Message must contain 'header' and 'body' sections")

        if not isinstance(data["header"], dict) or not isinstance(data["body"], dict):
            raise TypeError("'header' and 'body' must be dictionaries")

        return cls(header=data["header"], body=data["body"])

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert envelope to dictionary.

        Returns:
            Dict: Dictionary representation of the envelope
        """
        return {"header": self.header, "body": self.body}


# Common header fields and their descriptions
COMMON_HEADER_FIELDS = {
    "messageType": "",
    "schemaName": "",
    "schemaVersion": "Version of the message schema (optional)",
    "correlationId": "identifier linking related messages",
    "messageId": "Unique identifier for the message",
    "timestamp": "ISO 8601 timestamp when the message was created",
    "producer": "System or service that created the message",
    "contentType": "Content type of the body (optional)",
    "replyTo": "Topic to reply to (optional)",
    "priority": "Message priority (optional)",
    "ttl": "Time-to-live in seconds (optional)",
    "retryCount": "Number of retry attempts (optional, used internally)",
}
