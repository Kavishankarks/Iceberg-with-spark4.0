#!/bin/bash

# Script to run the Simple Kafka Streaming Application (writes to Parquet)

# Set JAVA_HOME to Java 17
export JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home
export PATH="$JAVA_HOME/bin:$PATH"

echo "🚀 Starting Simple Kafka Streaming Application"
echo "Using Java 17 from: $JAVA_HOME"

# Check if Kafka is running
echo "🔍 Checking Kafka connectivity..."
if ! nc -z localhost 9092; then
    echo "❌ Cannot connect to Kafka at localhost:9092"
    echo "💡 Please start Kafka first: ./start-kafka.sh"
    exit 1
fi

echo "✅ Kafka is accessible"
echo ""

echo "🔨 Building the project..."
mvn clean compile -q

if [ $? -eq 0 ]; then
    echo "✅ Build successful"
    echo ""
    echo "🌊 Starting simple Kafka streaming (Parquet output)..."
    echo "   Press Ctrl+C to stop"
    echo ""
    
    # Add JVM options for better performance and Java 17 compatibility
    export MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
    
    mvn exec:java@run-simple-streaming -q
else
    echo "❌ Build failed. Please check for compilation errors."
    exit 1
fi