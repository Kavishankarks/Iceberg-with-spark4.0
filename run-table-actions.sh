#!/bin/bash

# Script to run the Iceberg Table Actions Utility

# Set JAVA_HOME to Java 17
export JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home
export PATH="$JAVA_HOME/bin:$PATH"

echo "🔧 Starting Iceberg Table Actions Utility"
echo "Using Java 17 from: $JAVA_HOME"

echo "🔨 Building the project..."
mvn clean compile -q

if [ $? -eq 0 ]; then
    echo "✅ Build successful"
    echo ""
    echo "🚀 Starting Table Actions utility..."
    echo ""
    
    # Add JVM options for better performance and Java 17 compatibility
    export MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
    
    mvn exec:java@run-table-actions -q
else
    echo "❌ Build failed. Please check for compilation errors."
    exit 1
fi