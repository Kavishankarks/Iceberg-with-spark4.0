#!/bin/bash

# Script to run the Simple Spark SQL CLI (without Iceberg to avoid security issues)

echo "Building the project..."
mvn clean compile

if [ $? -eq 0 ]; then
    echo "Build successful. Starting Simple Spark CLI..."
    echo "Type 'help' for available commands, 'exit' to quit"
    echo ""
    mvn exec:java -Dexec.mainClass="com.example.spark.SimpleSparkCLI"
else
    echo "Build failed. Please check for compilation errors."
    exit 1
fi