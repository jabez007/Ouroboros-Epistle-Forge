"""
Centralised Kafka consumer configuration loaded from environment variables.

Example:
    >>> cfg = ConsumerConfig()
    >>> print(cfg.bootstrap_servers, cfg.group_id)
"""

import os
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class ConsumerConfig:
    """Validated environment-driven configuration for a Kafka consumer."""

    _TRUTHY_VALUES = {'true', '1', 'yes'}
    _FALSY_VALUES = {'false', '0', 'no'}
    _BOOLEAN_VALUES = _TRUTHY_VALUES | _FALSY_VALUES

    _VALID_OFFSET_RESET = ("earliest", "latest", "none")

    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "{{cookiecutter.default_bootstrap_servers}}")
    group_id: str = os.getenv("KAFKA_GROUP_ID", "{{cookiecutter.consumer_group_id}}")
    auto_offset_reset: str = os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest").casefold()
    auto_commit_offset: bool = field(init=False)

    def __post_init__(self): 
        # Convert string values to appropriate types
        raw_auto_commit = os.getenv("KAFKA_ENABLE_AUTO_COMMIT", "false")
        if raw_auto_commit.casefold() not in self._BOOLEAN_VALUES:
            raise ValueError("KAFKA_ENABLE_AUTO_COMMIT must be a boolean-like value")
        object.__setattr__(self, 'auto_commit_offset', self._str_to_bool(raw_auto_commit))

        # Validate values
        self._validate_config()

    def _str_to_bool(self, value: str) -> bool:
        """Convert string 'true'/'false' to boolean."""
        return value.casefold() in self._TRUTHY_VALUES
    
    def _validate_config(self) -> None:
        """Validate configuration values."""
        # Validate bootstrap_servers format
        if not self.bootstrap_servers:
            raise ValueError("KAFKA_BOOTSTRAP_SERVERS must not be empty")
        
        # Validate group_id is not empty
        if not self.group_id:
            raise ValueError("KAFKA_GROUP_ID must not be empty")
    
        # Validate auto_offset_reset is one of the expected values
        if self.auto_offset_reset not in self._VALID_OFFSET_RESET:
            raise ValueError(
                f"KAFKA_AUTO_OFFSET_RESET must be one of: {', '.join(self._VALID_OFFSET_RESET)}"
            )
