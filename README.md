# Ouroboros-Epistle-Forge

Inspired by the infinite cyclic nature of the Ouroboros, this toolkit streamlines the setup for scalable, efficient messaging workflows. Perfect for developers working with Kafka, RabbitMQ, or other queue systems.

This template provides a foundation for building Python applications that consume messages from Kafka topics using a dependency injection pattern. It's designed to handle multiple topics with dedicated handlers while maintaining consistent message structure and reliable processing.

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

- Python 3.8+
- Access to a Kafka cluster
- Poetry (recommended for dependency management)

### Installation

1. Install Cookiecutter:
   ```bash
   pip install cookiecutter
   ```

2. Generate a new project:
   ```bash
   cookiecutter gh:yourusername/kafka-consumer-template
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

Create a new handler by implementing the `MessageHandler` interface:

```python
from kafka_consumer.handlers import MessageHandler
from kafka_consumer.models import Message

class OrderCreatedHandler(MessageHandler):
    def __init__(self, some_dependency):
        self.some_dependency = some_dependency
        
    async def handle(self, message: Message) -> bool:
        # Process the message
        order_data = message.body
        
        try:
            # Your business logic here
            await self.some_dependency.process_order(order_data)
            return True
        except Exception as e:
            # Returning False will route to DLQ
            return False
```

### Registering Handlers

In your application's entry point:

```python
from kafka_consumer import Consumer
from kafka_consumer.handlers import MessageRouter
from your_app.handlers import OrderCreatedHandler

async def main():
    # Create dependencies
    order_service = OrderService()
    
    # Create handlers
    order_handler = OrderCreatedHandler(order_service)
    
    # Create router and register handlers
    router = MessageRouter()
    router.register("orders", order_handler)
    
    # Create and start consumer
    consumer = Consumer(
        bootstrap_servers="localhost:9092",
        group_id="my-consumer-group",
        topics=["orders", "shipments"],
        router=router
    )
    
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
├── kafka_consumer/
│   ├── __init__.py
│   ├── consumer.py         # Main consumer implementation
│   ├── handlers.py         # Handler interface and router
│   ├── models.py           # Message models and schemas
│   └── dlq.py              # Dead letter queue functionality
├── tests/
│   ├── __init__.py
│   ├── test_consumer.py
│   └── test_handlers.py
├── examples/
│   ├── simple_consumer.py
│   └── multi_topic_consumer.py
├── pyproject.toml          # Dependencies and project metadata
├── .env.example            # Example environment configuration
└── README.md               # This README
```

## Customization

### Message Validation

Validation is performed using Pydantic. Extend or modify the base message models in `models.py` to add custom validation rules:

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

