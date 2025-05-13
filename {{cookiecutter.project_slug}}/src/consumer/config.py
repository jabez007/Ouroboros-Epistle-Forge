"""
Centralised Kafka consumer configuration loaded from environment variables.

Example:
    >>> cfg = ConsumerConfig()
    >>> print(cfg.bootstrap_servers, cfg.group_id)
"""

import json
import os
from dataclasses import dataclass, field
from typing import Optional


@dataclass(frozen=True, slots=True)
class ConsumerConfig:
    """Validated environment-driven configuration for a Kafka consumer."""

    _TRUTHY_VALUES = {'true', '1', 'yes'}
    _FALSY_VALUES = {'false', '0', 'no'}
    _BOOLEAN_VALUES = _TRUTHY_VALUES | _FALSY_VALUES

    _VALID_OFFSET_RESET = frozenset({"earliest", "latest", "none"})

    bootstrap_servers: Optional[list[str]] = field(default_factory=lambda: None)
    group_id: str = field(default_factory=lambda: os.getenv("KAFKA_GROUP_ID", "{{cookiecutter.consumer_group_id}}"))
    auto_offset_reset: str = field(default_factory=lambda: os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest").casefold())
    auto_commit_offset: Optional[bool] = field(default_factory=lambda: None)

    def __post_init__(self):
        self._init_bootstrap_servers()

        self._init_auto_commit_offset()

        # Validate values
        self._validate_config()

    def _init_bootstrap_servers(self):
        if self.bootstrap_servers is None:
            # Get brokers from environment variable
            brokers_env = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "{{cookiecutter.default_bootstrap_servers}}")
            try:
                # Try to parse as JSON (for list format)
                brokers = json.loads(brokers_env)
                # If it's a string after parsing, put it in a list
                if isinstance(brokers, str):
                    brokers = [brokers]
                # If it's a list, keep it as is
                elif isinstance(brokers, list):
                    pass
                else:
                    # Raise error for unexpected types after JSON parsing
                    raise ValueError(f"KAFKA_BOOTSTRAP_SERVERS must be a string or list, got {type(brokers)}")
            except json.JSONDecodeError:
                # Not valid JSON, treat as a single broker string
                brokers = [brokers_env]

            object.__setattr__(self, 'bootstrap_servers', brokers)

    def _init_auto_commit_offset(self):
        if self.auto_commit_offset is None:
            # Convert string values to appropriate types
            raw_auto_commit = os.getenv("KAFKA_ENABLE_AUTO_COMMIT", "false")
            if raw_auto_commit.casefold() not in self._BOOLEAN_VALUES:
                raise ValueError("KAFKA_ENABLE_AUTO_COMMIT must be a boolean-like value")
            object.__setattr__(self, 'auto_commit_offset', self._str_to_bool(raw_auto_commit))

    def _str_to_bool(self, value: str) -> bool:
        """Convert string 'true'/'false' to boolean."""
        return value.casefold() in self._TRUTHY_VALUES
    
    def _validate_config(self) -> None:
        """Validate configuration values."""
        # Validate bootstrap_servers format
        if not self.bootstrap_servers:
            raise ValueError("KAFKA_BOOTSTRAP_SERVERS must not be empty")
        
        # Validate each broker string format
        for broker in self.bootstrap_servers:
            if not isinstance(broker, str) or not broker:
                raise ValueError(f"Each bootstrap server must be a non-empty string, got {broker}")
            # Optional: Add more strict validation if needed
            # if ":" not in broker:
            #     raise ValueError(f"Each bootstrap server should be in format 'host:port', got {broker}")

        # Validate group_id is not empty
        if not self.group_id:
            raise ValueError("KAFKA_GROUP_ID must not be empty")
    
        # Validate auto_offset_reset is one of the expected values
        if self.auto_offset_reset not in self._VALID_OFFSET_RESET:
            raise ValueError(
                f"KAFKA_AUTO_OFFSET_RESET must be one of: {', '.join(self._VALID_OFFSET_RESET)}"
            )
