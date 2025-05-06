"""
Message envelope model that follows a SOAP-like structure with header and body.
"""

from dataclasses import dataclass
from typing import Any, Dict

# Common header fields and their descriptions
COMMON_HEADER_FIELDS = {
    "messageType": "Type of message (e.g., 'event', 'command', 'notification')",
    "schemaName": "Name of the schema defining the message structure",
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


@dataclass(slots=True)
class MessageEnvelope:
    """
    Message envelope with header and body sections.

    The header contains metadata about the message that is consistent
    across all topics. The body contains topic-specific data.
    """

    _REQUIRED_HEADERS = ("messageType", "schemaName", "correlationId", "messageId", "timestamp", "producer")

    header: Dict[str, Any]
    body: Dict[str, Any]

    def __post_init__(self) -> None:
        self._validate_header(self.header)

    def _validate_header(self, header: dict) -> None:
        # define required and allowed headers 
        missing = [k for k in self._REQUIRED_HEADERS if k not in header]
        if missing:
            raise ValueError(f"Missing required header fields: {missing}")

        # catch any unexpected header keys
        unexpected = [k for k in header if k not in COMMON_HEADER_FIELDS]
        if unexpected:
            raise ValueError(f"Unexpected header fields: {unexpected}")

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
            TypeError: If header or body is not a dictionary
        """

        # A schema might legitimately allow {} in the body (e.g., a ping event).
        if "header" not in data or "body" not in data or not data["header"]:
            raise ValueError("Message must contain 'header' and 'body' sections")

        # Header must be truthy because required keys will be validated later
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
