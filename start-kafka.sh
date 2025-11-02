#!/bin/bash

# Script to start Kafka cluster using Docker Compose

echo "Starting Kafka Cluster for Spark-Iceberg Integration"

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "Docker is not running. Please start Docker first."
    exit 1
fi

# Check if docker-compose is available
if ! command -v docker-compose &> /dev/null; then
    echo "docker-compose not found. Please install Docker Compose."
    exit 1
fi

echo "🔧 Starting Kafka services..."
docker-compose up -d

# Wait for services to be ready
echo "⏳ Waiting for Kafka to be ready..."
sleep 30

# Check if Kafka is ready
echo "🔍 Checking Kafka status..."
until docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1; do
    echo "⏳ Waiting for Kafka to be fully ready..."
    sleep 5
done

echo "✅ Kafka cluster is ready!"
echo ""
echo "Service URLs:"
echo "  • Kafka Bootstrap Server: localhost:9092"
echo "  • Kafka UI: http://localhost:8080"
echo "  • Kafka Connect: http://localhost:8083"
echo "  • Zookeeper: localhost:2181"
echo ""

# Create the topic for user events
echo "📝 Creating Kafka topic 'user-events'..."
docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
    --create --topic user-events \
    --partitions 3 \
    --replication-factor 1 \
    --if-not-exists

echo "✅ Topic 'user-events' created successfully"
echo ""

# Show topic details
echo "🔍 Topic details:"
docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
    --describe --topic user-events

echo ""
echo "🎉 Kafka setup complete!"
echo ""
echo "💡 Next steps:"
echo "  1. Start the data producer: ./run-kafka-producer.sh"
echo "  2. Start the streaming app: ./run-kafka-streaming.sh"
echo "  3. Monitor with Kafka UI: http://localhost:8080"
echo ""
echo "🛑 To stop Kafka: docker-compose down"