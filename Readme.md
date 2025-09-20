# Spark Iceberg Overview

A comprehensive project demonstrating Apache Spark with Apache Iceberg table creation, management, streaming ingestion, and exploration tools.

## Project Overview

This project provides multiple ways to interact with Iceberg tables using Apache Spark 4.0, including:

- **Interactive Java CLI** for running Spark SQL queries
- **Kafka streaming ingestion** for real-time data pipelines
- **Metadata exploration tools** for inspecting Iceberg table internals
- **Web-based Streamlit interface** for visual data exploration
- **Comprehensive examples** of Iceberg features and operations

## Prerequisites

- **Java 17** (Amazon Corretto or OpenJDK)
- **Apache Spark 4.0** 
- **Apache Iceberg 1.10.0**
- **Apache Kafka 3.7** (for streaming)
- **Docker & Docker Compose** (for Kafka cluster)
- **Python 3.8+** (for Streamlit app)
- **Maven 3.6+**

## 🚀 Quick Start

### 1. Interactive Spark SQL CLI
Run Spark queries directly with Iceberg table support:
```bash
./run-cli.sh
```

### 2. Streamlit Web Interface
Visual interface for metadata and Avro file exploration:
```bash
./run-streamlit.sh
```

### 3. Java Metadata Viewer
Command-line tool for inspecting Iceberg metadata:
```bash
./run-metadata-viewer.sh
```

### 4. Comprehensive Demo
Run the full Iceberg feature demonstration:
```bash
./run.sh
```

## 🌊 Kafka Streaming to Iceberg

This project includes a complete real-time streaming pipeline that ingests data from Apache Kafka into Iceberg tables using Spark Structured Streaming.

### Kafka Pipeline Overview

```
Kafka Topic (user-events) → Spark Streaming → Parquet Files → Iceberg Tables
```

### 🚀 Quick Start - Streaming Pipeline

#### Step 1: Start Kafka Cluster
```bash
# Start Kafka, Zookeeper, and Kafka UI using Docker Compose
./start-kafka.sh
```

This will start:
- **Kafka**: `localhost:9092`
- **Zookeeper**: `localhost:2181`
- **Kafka UI**: `http://localhost:8080`
- **Kafka Connect**: `localhost:8083`

#### Step 2: Generate Sample Data
```bash
# Generate 100 user events with 500ms intervals
./run-kafka-producer.sh 100 500

# Or use default settings (1000 events, 100ms intervals)
./run-kafka-producer.sh
```

#### Step 3: Start Streaming to Parquet Files
```bash
# Start the streaming application
./run-simple-streaming.sh
```

**Note**: Direct Iceberg streaming has ANTLR version conflicts with Spark 4.0. The simple streaming version writes to Parquet files which can then be loaded into Iceberg tables.

### 📊 Data Pipeline Components

#### Kafka Data Producer (`KafkaDataProducer.java`)
Generates realistic user event data including:
- **User Events**: login, logout, page_view, search, purchase
- **User Details**: randomized names, departments, locations
- **Event Metadata**: timestamps, session IDs, IP addresses
- **E-commerce Data**: products, prices, quantities, categories

**Sample Event Structure**:
```json
{
  "event_id": 12345,
  "timestamp": "2025-01-20T14:30:45",
  "event_type": "purchase",
  "user_id": 1001,
  "user_name": "Alice Johnson",
  "department": "Engineering",
  "location": "San Francisco",
  "session_id": "sess_abc123",
  "ip_address": "192.168.1.100",
  "amount": 99.99,
  "currency": "USD",
  "product_id": "PROD_12345",
  "category": "Electronics"
}
```

#### Spark Structured Streaming (`SimpleKafkaToIcebergStreaming.java`)
- **Real-time Processing**: 10-second micro-batches
- **JSON Parsing**: Automatic schema inference and validation
- **Monitoring**: Built-in streaming metrics and progress tracking
- **Fault Tolerance**: Checkpointing for exactly-once processing
- **Output**: Parquet files in `./warehouse/streaming/user_events_parquet`

### 🛠 Advanced Configuration

#### Customize Producer Settings
```bash
# Generate specific number of events with custom interval
./run-kafka-producer.sh [number_of_events] [interval_ms]

# Examples:
./run-kafka-producer.sh 50 1000    # 50 events, 1 second apart
./run-kafka-producer.sh 500 100    # 500 events, 100ms apart
```

#### Docker Compose Services
The `docker-compose.yml` includes:
```yaml
services:
  zookeeper:     # Kafka coordination
  kafka:         # Message broker
  kafka-ui:      # Web UI for Kafka management
  kafka-connect: # Integration platform
```

#### Kafka Topic Configuration
- **Topic**: `user-events`
- **Partitions**: 3
- **Replication Factor**: 1
- **Auto-creation**: Enabled

### 📈 Monitoring and Observability

#### Kafka UI Dashboard
Access the Kafka UI at `http://localhost:8080` to:
- View topic messages and schemas
- Monitor consumer group lag
- Inspect partition details
- Manage Kafka Connect connectors

#### Streaming Application Metrics
The streaming app provides real-time monitoring:
```
📈 Streaming Stats - Input: 100.0 rows/sec, Processed: 95.2 rows/sec, Batch Duration: 2500ms
📊 Processing events from Kafka topic: user-events
📁 Writing to Parquet files: ./warehouse/streaming/user_events_parquet
⏱️  Trigger interval: 10 seconds
---
```

#### Spark Web UI
Monitor Spark jobs at `http://localhost:4040` (or next available port):
- Streaming tab for query progress
- Jobs tab for task execution
- Storage tab for RDD/DataFrame caching

### 🔧 Pipeline Management

#### Start Complete Pipeline
```bash
# Terminal 1: Start infrastructure
./start-kafka.sh

# Terminal 2: Start streaming consumer
./run-simple-streaming.sh

# Terminal 3: Generate data
./run-kafka-producer.sh 1000 200
```

#### Stop Pipeline
```bash
# Stop streaming (Ctrl+C in streaming terminal)
# Stop Kafka cluster
docker-compose down

# Clean up checkpoints (optional)
rm -rf ./checkpoint
```

#### Data Output Locations
- **Streaming Data**: `./warehouse/streaming/user_events_parquet/`
- **Checkpoint**: `./checkpoint/`
- **Kafka Logs**: Docker volumes (`kafka-data`, `zk-data`)

### 🚨 Known Issues & Workarounds

#### ANTLR Version Conflict
- **Issue**: Spark 4.0 and Iceberg have incompatible ANTLR versions
- **Workaround**: Use `SimpleKafkaToIcebergStreaming` for Parquet output
- **Future**: Will be resolved in upcoming Iceberg releases

#### Memory Considerations
```bash
# Increase JVM memory for large datasets
export MAVEN_OPTS="-Xmx4g -Xms2g --add-opens=java.base/java.nio=ALL-UNNAMED"
```

#### Docker Resource Requirements
- **Minimum**: 4GB RAM, 2 CPU cores
- **Recommended**: 8GB RAM, 4 CPU cores
- **Storage**: 10GB available disk space

### 🎯 Use Cases

This streaming pipeline is ideal for:
- **Real-time Analytics**: Process user behavior as it happens
- **Event Sourcing**: Capture all user interactions for replay
- **Data Lake Ingestion**: Continuous data loading into data lakes
- **Monitoring & Alerting**: React to events in real-time
- **A/B Testing**: Stream experiment results for analysis

### 📝 Next Steps

1. **Load Parquet to Iceberg**: Convert streaming output to Iceberg tables
2. **Schema Evolution**: Handle changing event schemas gracefully
3. **Partitioning Strategy**: Optimize for query patterns
4. **Compaction**: Manage small file problems
5. **Time Travel**: Leverage Iceberg's temporal capabilities

## 🔧 Features

### Interactive SQL CLI (`SparkIcebergCLI.java`)
- Real-time Spark SQL query execution
- Iceberg table operations (CREATE, INSERT, SELECT, ALTER)
- Time travel queries
- Schema evolution
- Clean error handling with `getMessage()` extraction

### Streamlit Web App
- **Metadata Browser**: Visual exploration of Iceberg table metadata
- **Avro File Viewer**: Load and analyze Avro data files
- **Table Statistics**: Warehouse-wide overview and metrics
- **Interactive Filtering**: Query data through web interface

### Metadata Tools
- **Java Metadata Viewer**: CLI tool for inspecting metadata files
- **Simple Metadata Viewer**: Lightweight version without Spark dependencies
- **Schema Analysis**: Field types, constraints, and evolution tracking
- **Snapshot History**: View all table operations and changes

### Educational Examples
- Table creation with partitioning
- Data insertion and querying
- Snapshot management
- Schema evolution
- Time travel capabilities
- Manifest file exploration

## 🛠 Configuration

### Java Environment
The project is configured for Java 17. Update `JAVA_HOME` in run scripts:
```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home
```

### Spark Configuration
- **Format**: Parquet with Snappy compression
- **Catalog**: Hadoop-based file system catalog
- **Warehouse**: `./warehouse` directory
- **Logging**: Minimal output for clean CLI experience

### Python Dependencies
For the Streamlit app:
```bash
streamlit>=1.28.0
pandas>=2.0.0
pyarrow>=12.0.0
numpy>=1.24.0
fastavro>=1.8.0
```

## 📚 Usage Examples

### Creating and Querying Tables
```sql
spark-sql> CREATE TABLE local.demo.users (
    id BIGINT,
    name STRING,
    email STRING,
    created_date DATE
) USING iceberg
PARTITIONED BY (created_date);

spark-sql> INSERT INTO local.demo.users VALUES 
(1, 'John Doe', 'john@example.com', DATE('2023-01-15'));

spark-sql> SELECT * FROM local.demo.users;
```

### Time Travel Queries
```sql
spark-sql> SELECT * FROM local.demo.users.snapshots;
spark-sql> SELECT * FROM local.demo.users TIMESTAMP AS OF '2023-01-01';
```

### Schema Evolution
```sql
spark-sql> ALTER TABLE local.demo.users ADD COLUMN phone STRING;
spark-sql> DESCRIBE local.demo.users;
```

## 🏗 Architecture

### Java Components
- **Spark 4.0**: Latest Spark version with enhanced Iceberg support
- **Iceberg 1.10.0**: Advanced table format with ACID transactions
- **Maven**: Dependency management and build system
- **Log4j**: Configurable logging with minimal verbosity

### Python Components
- **Streamlit**: Interactive web interface
- **FastAvro**: High-performance Avro file reading
- **Pandas**: Data manipulation and analysis
- **PyArrow**: Columnar data processing

### Storage Format
- **Iceberg Tables**: ACID-compliant table format
- **Parquet Files**: Efficient columnar storage
- **Manifest Files**: Metadata tracking for data files
- **Snapshot System**: Time travel and versioning

## 🔧 Troubleshooting

### Common Issues

**Java Version Conflicts**
```bash
# Check current Java version
java -version

# Set correct JAVA_HOME
export JAVA_HOME=/path/to/java17
```

**Memory Issues**
```bash
# Increase JVM memory in run scripts
export MAVEN_OPTS="-Xmx4g -Xms2g"
```

**Iceberg Schema Errors**
- Ensure proper DATE() casting for date fields
- Use compatible data types for schema evolution
- Check Iceberg format version compatibility

**Streamlit Connection Issues**
- Verify Python environment and dependencies
- Check file permissions for warehouse directory
- Ensure Avro files are valid format

## Learn More

### Iceberg Documentation
- [Apache Iceberg Official Docs](https://iceberg.apache.org/)
- [Iceberg Table Format Specification](https://iceberg.apache.org/spec/)
- [Spark-Iceberg Integration](https://iceberg.apache.org/docs/latest/spark-configuration/)

### Spark Resources
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Spark SQL Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test with provided examples
5. Submit a pull request

## License

This project is provided as educational material for learning Spark and Iceberg integration.

---

*Built with️ for the Apache Spark and Iceberg community*
