#!/bin/bash

# Script to run the Spark Iceberg example

echo "Building the project..."
mvn clean compile

if [ $? -eq 0 ]; then
    echo "Build successful. Running the Iceberg example..."
    mvn exec:java -Dexec.mainClass="com.example.spark.IcebergTableExample"
else
    echo "Build failed. Please check for compilation errors."
    exit 1
fi