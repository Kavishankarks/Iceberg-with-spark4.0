#!/bin/bash

# Script to run the Iceberg Metadata Viewer
# Set JAVA_HOME to Java 17
export JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home
export PATH="$JAVA_HOME/bin:$PATH"

echo "=== Iceberg Metadata Viewer ==="
echo "Using Java 17 from: $JAVA_HOME"

# Default metadata path
DEFAULT_PATH="$(pwd)/warehouse/demo/employees/metadata"

if [ $# -eq 0 ]; then
    METADATA_PATH="$DEFAULT_PATH"
    echo "Using default metadata path: $METADATA_PATH"
else
    METADATA_PATH="$1"
    echo "Using provided metadata path: $METADATA_PATH"
fi

echo "Building the project..."
mvn clean compile -q

if [ $? -eq 0 ]; then
    echo "Build successful. Starting Metadata Viewer..."
    echo ""
    # Use java directly to avoid any Maven/Spark configuration interference
    java -cp "target/classes:$(mvn dependency:build-classpath -q -Dmdep.outputFile=/dev/stdout)" \
         com.example.spark.IcebergMetadataViewer "$METADATA_PATH"
else
    echo "Build failed. Please check for compilation errors."
    exit 1
fi