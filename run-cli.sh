#!/bin/bash

# Script to run the Spark Iceberg Interactive CLI

# Set JAVA_HOME to Java 17
export JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home
export PATH="$JAVA_HOME/bin:$PATH"

echo "Using Java 17 from: $JAVA_HOME"
echo "Java version: $(java -version 2>&1 | head -n 1)"


echo "Building the project..."
mvn clean compile

if [ $? -eq 0 ]; then
    echo "Build successful. Starting Spark Iceberg CLI..."
    echo "Type 'help' for available commands, 'exit' to quit"
    echo ""
    export MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
    mvn exec:java -Dexec.mainClass="com.example.spark.SparkIcebergCLI"
else
    echo "Build failed. Please check for compilation errors."
    exit 1
fi