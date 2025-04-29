import os


class ConsumerConfig:

    def __init__(self):
        self.bootstrap_servers = os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", {{cookiecutter.default_bootstrap_servers}}
        )
        self.group_id = os.getenv("KAFKA_GROUP_ID", {{cookiecutter.consumer_group_id}})
        self.auto_offset_reset = os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest")
        self.auto_commit_offset = os.getenv("KAFKA_ENABLE_AUTO_COMMIT", "false")
