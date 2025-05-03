import os


class ConsumerConfig:

    def __init__(self):
        self.bootstrap_servers = os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "{{cookiecutter.default_bootstrap_servers}}"
        )
        self.group_id = os.getenv("KAFKA_GROUP_ID", "{{cookiecutter.consumer_group_id}}")
        self.auto_offset_reset = os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest")
        self.auto_commit_offset = os.getenv("KAFKA_ENABLE_AUTO_COMMIT", "false")
        
        # Convert string values to appropriate types
        self.auto_commit_offset = self._str_to_bool(self.auto_commit_offset)
        
        # Validate values
        self._validate_config()

    def _str_to_bool(self, value: str) -> bool:
        """Convert string 'true'/'false' to boolean."""
        return value.lower() in ('true', '1', 'yes')
    
    def _validate_config(self) -> None:
        """Validate configuration values."""
        # Validate bootstrap_servers format
        if not self.bootstrap_servers:
            raise ValueError("KAFKA_BOOTSTRAP_SERVERS must not be empty")
        
        # Validate group_id is not empty
        if not self.group_id:
            raise ValueError("KAFKA_GROUP_ID must not be empty")
    
        # Validate auto_offset_reset is one of the expected values
        valid_offset_reset = ["earliest", "latest", "none"]
        if self.auto_offset_reset not in valid_offset_reset:
            raise ValueError(
                f"KAFKA_AUTO_OFFSET_RESET must be one of: {', '.join(valid_offset_reset)}"
            )
