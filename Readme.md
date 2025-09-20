# Spark Iceberg Overview

A comprehensive project demonstrating Apache Spark with Apache Iceberg table creation, management, and exploration tools.

## Project Overview

This project provides multiple ways to interact with Iceberg tables using Apache Spark 4.0, including:

- **Interactive Java CLI** for running Spark SQL queries
- **Metadata exploration tools** for inspecting Iceberg table internals
- **Web-based Streamlit interface** for visual data exploration
- **Comprehensive examples** of Iceberg features and operations

## Prerequisites

- **Java 17** (Amazon Corretto or OpenJDK)
- **Apache Spark 4.0** 
- **Apache Iceberg 1.10.0**
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
