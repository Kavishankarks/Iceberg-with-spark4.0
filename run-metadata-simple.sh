#!/bin/bash

# Simple script to run the Iceberg Metadata Viewer without Spark dependencies

# Set JAVA_HOME to Java 17
export JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home
export PATH="$JAVA_HOME/bin:$PATH"

echo "=== Iceberg Metadata Viewer (Simple) ==="
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

echo "Compiling simple metadata viewer..."

# Create a minimal classpath with just Jackson
JACKSON_VERSION="2.17.2"
CLASSPATH="target/classes"

# Download Jackson if not present
JACKSON_DIR="target/jackson-libs"
mkdir -p "$JACKSON_DIR"

if [ ! -f "$JACKSON_DIR/jackson-core-$JACKSON_VERSION.jar" ]; then
    echo "Downloading Jackson libraries..."
    curl -s -o "$JACKSON_DIR/jackson-core-$JACKSON_VERSION.jar" \
        "https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-core/$JACKSON_VERSION/jackson-core-$JACKSON_VERSION.jar"
    curl -s -o "$JACKSON_DIR/jackson-databind-$JACKSON_VERSION.jar" \
        "https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-databind/$JACKSON_VERSION/jackson-databind-$JACKSON_VERSION.jar"
    curl -s -o "$JACKSON_DIR/jackson-annotations-$JACKSON_VERSION.jar" \
        "https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-annotations/$JACKSON_VERSION/jackson-annotations-$JACKSON_VERSION.jar"
fi

CLASSPATH="$CLASSPATH:$JACKSON_DIR/jackson-core-$JACKSON_VERSION.jar:$JACKSON_DIR/jackson-databind-$JACKSON_VERSION.jar:$JACKSON_DIR/jackson-annotations-$JACKSON_VERSION.jar"

# Compile just the metadata viewer
mkdir -p target/classes
javac -cp "$CLASSPATH" -d target/classes src/main/java/com/example/spark/IcebergMetadataViewer.java

if [ $? -eq 0 ]; then
    echo "Compilation successful. Starting Metadata Viewer..."
    echo ""
    java -cp "$CLASSPATH" com.example.spark.IcebergMetadataViewer "$METADATA_PATH"
else
    echo "Compilation failed."
    exit 1
fi