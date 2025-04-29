# Ouroboros-Epistle-Forge

Inspired by the infinite cyclic nature of the Ouroboros, this toolkit streamlines the setup for scalable, efficient messaging workflows.
Perfect for developers working with Kafka, RabbitMQ, or other queue systems.

This template provides a foundation for building Python applications that consume messages from Kafka topics using a dependency injection pattern.
It's designed to handle multiple topics with dedicated handlers while maintaining consistent message structure and reliable processing.

## Features

- 📦 **Dependency Injection Pattern**: Register handlers for specific topics
- 🔄 **Multi-Topic Support**: Subscribe to and process messages from multiple Kafka topics
- 📄 **Standardized Message Format**: Uses envelope pattern with consistent headers and topic-specific bodies
- ✅ **Manual Offset Commit**: Ensures messages are fully processed before being acknowledged
- ⚠️ **Dead Letter Queue Support**: Automatic routing of failed messages to topic-specific DLQs
- 🏗️ **Cookiecutter Template**: Quickly scaffold new consumer applications

## Message Structure

All messages follow a consistent envelope pattern:

```json
{
  "header": {
    "messageType": "string",
    "schemaName": "string",
    "schemaVersion": "string",
    "correlationId": "string",
    "messageId": "string",
    "timestamp": "ISO-8601 timestamp",
    "producer": "string"
  },
  "body": {
    // Topic-specific payload
  }
}
```

## Getting Started

### Prerequisites

- Python 3.9+
- Access to a Kafka cluster
- Poetry (recommended for dependency management)

### Installation

1. Install Cookiecutter:
   ```bash
   pip install cookiecutter
   ```

2. Generate a new project:
   ```bash
   cookiecutter gh:jabez007/ouroboros-epistle-forge
   ```

3. Install dependencies:
   ```bash
   cd your-project-name
   poetry install
   ```

### Configuration

Create a `.env` file based on the provided `.env.example`:

```
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_GROUP_ID=consumer-group-1
KAFKA_AUTO_OFFSET_RESET=earliest
KAFKA_ENABLE_AUTO_COMMIT=false
```

## Usage

### Defining a Handler

Create a new handler by implementing the `BaseHandler` interface:

```python
""" In the `handlers` module """
from .base import BaseHandler
from ..models.envelope import MessageEnvelope
from ..models.orders import CreateOrder

class OrderCreatedHandler(BaseHandler):
    
    def __init__(self, some_dependency):
        self.some_dependency = some_dependency

    async def _process_message(self, message: MessageEnvelope) -> bool:
        # Process the message
        order_data = CreatedOrder.from_dict(message.body)
        
        try:
            # Your business logic here
            await self.some_dependency.process_order(order_data)
            # return True to commit offset
            return True
        except Exception as e:
            if (e.message = "try again later"):
                # return False to skip commiting offset
                return False
            else:
                # Raise exception to route to DLQ
                raise e 
```

### Registering Handlers

In your application's entry point:

```python
from .consumer import KafkaConsumer
from .handlers import OrderCreatedHandler
from .services import OrderService

async def main():
    # Create dependencies
    order_service = OrderService()
    
    # Create handlers
    order_handler = OrderCreatedHandler(order_service)
    
    # Create consumer
    consumer = KafkaConsumer(
        bootstrap_servers="localhost:9092",
        group_id="my-consumer-group"
    )

    # Register handlers
    consumer.register_handler("orders", order_handler)
    
    # Start consumer
    await consumer.start()

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
```

## Message Processing Flow

1. Consumer subscribes to configured topics
2. Messages are received and deserialized
3. Message is routed to the appropriate handler based on topic
4. If handler returns `True`, offset is committed
5. If handler returns `False` or raises an exception, message is sent to DLQ and offset is committed

## Dead Letter Queue

Failed messages are automatically published to a DLQ named after the original topic:
- Messages from `orders` go to `orders.dlq`
- Messages from `shipments` go to `shipments.dlq`

The original message is wrapped in an error envelope:

```json
{
  "original_message": {
    "header": { ... },
    "body": { ... }
  },
  "error": {
    "timestamp": "ISO-8601 timestamp",
    "reason": "Error message if available"
  }
}
```

## Project Structure

```
your-project-name/
├── src/
│   ├── main.py                # Application entry point
│   ├── consumer.py            # Main consumer implementation
│   ├── handlers/              # Handler interface and router
│   │   ├── __init__.py
│   │   ├── topic1_handler.py
│   │   └── topic2_handler.py
│   └── models/                # Message models and schemas
│       ├── __init__.py
│       ├── topic1_model.py
│       └── topic2_model.py
├── tests/
│   ├── __init__.py
│   ├── test_consumer.py
│   ├── test_topic1_handler.py
│   └── test_topic2_handler.py
├── pyproject.toml             # Dependencies and project metadata
├── .env                       # Environment configuration
└── README.md                  # Project README
```

## Customization

### Message Validation

Validation is performed using Pydantic. Extend or modify the base message models in `models` to add custom validation rules:

```python
from kafka_consumer.models import BaseMessage, Header

class CustomMessage(BaseMessage):
    class Config:
        extra = "forbid"  # Prevents additional fields
        
    @validator("header")
    def validate_required_fields(cls, header):
        if not header.correlation_id:
            raise ValueError("correlation_id is required")
        return header
```

### Custom Serialization

The default serialization is JSON, but you can customize it:

```python
from kafka_consumer import Consumer
from your_app.serialization import CustomSerializer

consumer = Consumer(
    # ... other settings
    serializer=CustomSerializer()
)
```

## Advanced Configuration

### Concurrency Control

Control handler concurrency:

```python
consumer = Consumer(
    # ... other settings
    max_workers=10,  # Maximum concurrent handlers
    max_poll_records=500  # Maximum records per poll
)
```

### Error Handling

Customize error handling:

```python
from kafka_consumer import Consumer, ErrorPolicy

consumer = Consumer(
    # ... other settings
    error_policy=ErrorPolicy.CONTINUE  # Continue processing on errors
)
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the GNU GPLv3 License - see the LICENSE file for details.

