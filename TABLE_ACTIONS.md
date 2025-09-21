# Iceberg Table Actions Reference

This document provides a comprehensive guide to Apache Iceberg table maintenance operations using the Java Actions API.

## Overview

Due to ANTLR version conflicts between Spark 4.0 and Iceberg 1.10.0, SQL-based `CALL` procedures don't work. This utility provides access to all Iceberg maintenance operations through the Java Actions API.

## Getting Started

### Start the Actions Utility
```bash
./run-table-actions.sh
```

### Available Tables
The utility automatically lists available tables on startup:
```
📚 Available Tables in Warehouse:
   📂 Database: demo
      📋 Table: demo.employees
   📂 Database: streaming
      📋 Table: streaming.user_events
```

## Core Table Actions

### 1. Rewrite Data Files (Compaction)

**Purpose**: Combine small files into larger ones for better query performance.

**Command**:
```bash
iceberg-actions> rewrite-data demo.employees
```

**What it does**:
- Combines small data files into larger ones (128MB target)
- Improves query performance by reducing file overhead
- Reduces metadata overhead in the table
- Uses parallel processing for better performance

**Configuration Options** (in `IcebergTableActions.java`):
```java
.option("min-input-files", "2")              // Minimum files to trigger rewrite
.option("target-file-size-bytes", "134217728") // 128MB target size
.option("max-concurrent-file-group-rewrites", "4") // Parallel rewrites
.option("partial-progress.enabled", "true")    // Allow partial completion
```

**Output Example**:
```
🔄 Starting data files rewrite...
✅ Data files rewrite completed!
📊 Results:
   • Files rewritten: 15
   • Files added: 3
   • Bytes rewritten: 2.1 GB
   • Files groups: 5
```

**When to use**:
- After many small writes/inserts
- When query performance degrades
- As part of regular maintenance schedule

---

### 2. Rewrite Manifests

**Purpose**: Optimize manifest files for better metadata performance.

**Command**:
```bash
iceberg-actions> rewrite-manifests demo.employees
```

**What it does**:
- Consolidates manifest files
- Reduces manifest list scanning time
- Improves query planning performance
- Optimizes metadata access patterns

**Configuration Options**:
```java
.option("use-caching", "true")  // Enable manifest caching
```

**Output Example**:
```
🔄 Starting manifest rewrite...
✅ Manifest rewrite completed!
📊 Results:
   • Manifests added: 2
```

**When to use**:
- After many commits/writes
- When metadata queries are slow
- As part of regular maintenance

---

### 3. Expire Snapshots

**Purpose**: Remove old snapshots to free up storage and reduce metadata size.

**Command**:
```bash
iceberg-actions> expire-snapshots demo.employees
```

**What it does**:
- Removes snapshots older than 7 days (configurable)
- Deletes associated data files no longer referenced
- Reduces metadata size
- Frees up storage space

**Configuration** (in code):
```java
long expireTimestamp = System.currentTimeMillis() - (7L * 24 * 60 * 60 * 1000); // 7 days
.expireOlderThan(expireTimestamp)
.option("stream-results", "true")
```

**Output Example**:
```
🔄 Starting snapshot expiration...
✅ Snapshot expiration completed!
```

**When to use**:
- Regular cleanup (weekly/monthly)
- When storage costs are high
- To reduce metadata overhead

**⚠️ Warning**: This permanently deletes data. Ensure you don't need time-travel to expired snapshots.

---

### 4. Remove Orphan Files

**Purpose**: Clean up files that are no longer referenced by any snapshot.

**Command**:
```bash
iceberg-actions> remove-orphan-files demo.employees
```

**What it does**:
- Identifies files older than 3 days not referenced by table
- Safely removes orphaned data files
- Frees up storage space
- Cleans up failed write operations

**Configuration**:
```java
long olderThan = System.currentTimeMillis() - (3L * 24 * 60 * 60 * 1000); // 3 days
.olderThan(olderThan)
```

**Output Example**:
```
🔄 Starting orphan file removal...
✅ Orphan file removal completed!
📊 Results:
   • Orphan files found: 23
```

**When to use**:
- After failed write operations
- Regular cleanup maintenance
- When storage contains unreferenced files

---

### 5. Show Table Information

**Purpose**: Display comprehensive table metadata and statistics.

**Command**:
```bash
iceberg-actions> show-table-info demo.employees
```

**What it shows**:
- **Schema**: Column names, types, and constraints
- **Properties**: Table configuration settings
- **Snapshots**: Recent table versions and commit history
- **Data Files**: File locations, formats, and sizes

**Output Example**:
```
📋 Table Information for: demo.employees

🏗️  Schema:
+----------+---------+-------+
|  col_name|data_type|comment|
+----------+---------+-------+
|        id|   bigint|   null|
|      name|   string|   null|
| hire_date|     date|   null|
+----------+---------+-------+

⚙️  Properties:
+--------------------+--------------------+
|                 key|               value|
+--------------------+--------------------+
|write.format.default|             parquet|
|write.parquet.com...|              snappy|
+--------------------+--------------------+

📸 Snapshots:
+--------------------+--------------------+--------------------+
|         snapshot_id|        committed_at|             summary|
+--------------------+--------------------+--------------------+
|   1234567890123456|2025-01-20 10:30:00|    {added-records=5}|
+--------------------+--------------------+--------------------+

📁 Data Files:
+--------------------+-----------+------------+-------------------+
|           file_path|file_format|record_count|file_size_in_bytes|
+--------------------+-----------+------------+-------------------+
|/warehouse/demo/e...|    PARQUET|         100|             45231|
+--------------------+-----------+------------+-------------------+
```

**When to use**:
- Debugging table issues
- Understanding table structure
- Monitoring table growth
- Planning maintenance operations

---

### 6. List Tables

**Purpose**: Display all available Iceberg tables in the warehouse.

**Command**:
```bash
iceberg-actions> list-tables
```

**Output Example**:
```
📚 Available Tables in Warehouse:
   📂 Database: demo
      📋 Table: demo.employees
      📋 Table: demo.customers
   📂 Database: streaming
      📋 Table: streaming.user_events
      📋 Table: streaming.metrics
```

**When to use**:
- Discovering available tables
- Verifying table existence
- Planning maintenance across multiple tables

---

## SQL Equivalent Commands

| Action API Command | Equivalent SQL CALL (if working) |
|---|---|
| `rewrite-data demo.employees` | `CALL iceberg.system.rewrite_data_files('demo.employees', map('target-file-size-bytes', '134217728'))` |
| `rewrite-manifests demo.employees` | `CALL iceberg.system.rewrite_manifests('demo.employees')` |
| `expire-snapshots demo.employees` | `CALL iceberg.system.expire_snapshots('demo.employees', TIMESTAMP '2025-01-13 00:00:00')` |
| `remove-orphan-files demo.employees` | `CALL iceberg.system.remove_orphan_files('demo.employees')` |

## Advanced Configuration

### Customizing Rewrite Settings

Edit `IcebergTableActions.java` to modify default behaviors:

```java
// Increase target file size to 256MB
.option("target-file-size-bytes", "268435456")

// Require more files before rewriting
.option("min-input-files", "5") 

// Increase parallelism
.option("max-concurrent-file-group-rewrites", "8")
```

### Snapshot Retention Policy

```java
// Change retention from 7 days to 30 days
long expireTimestamp = System.currentTimeMillis() - (30L * 24 * 60 * 60 * 1000);
```

### Orphan File Age Threshold

```java
// Change from 3 days to 1 day
long olderThan = System.currentTimeMillis() - (1L * 24 * 60 * 60 * 1000);
```

## Best Practices

### 📅 Maintenance Schedule

**Daily**:
- Monitor table growth with `show-table-info`
- Check for failed operations

**Weekly**:
- Run `rewrite-data` on frequently updated tables
- Execute `rewrite-manifests` for metadata optimization

**Monthly**:
- Run `expire-snapshots` to clean old versions
- Execute `remove-orphan-files` for storage cleanup

### ⚡ Performance Optimization

**File Size Targets**:
- **Small tables** (< 1GB): 64MB files
- **Medium tables** (1-100GB): 128MB files  
- **Large tables** (> 100GB): 256MB+ files

**Rewrite Triggers**:
- More than 20 files per partition
- Files smaller than 32MB
- After bulk insert operations

### 🔒 Safety Considerations

**Before expire-snapshots**:
- Verify no critical time-travel queries depend on old snapshots
- Ensure backups are available if needed
- Test on non-production tables first

**Before remove-orphan-files**:
- Wait at least 3 days after failed operations
- Verify files are truly orphaned
- Check for concurrent write operations

## Troubleshooting

### Common Issues

**"Table does not exist"**:
- Check table name format: `database.table` not `catalog.database.table`
- Verify table exists: use `list-tables`
- Ensure warehouse path is correct

**"No files to rewrite"**:
- Files may already be optimal size
- Adjust `min-input-files` setting
- Check actual file sizes with `show-table-info`

**Out of memory errors**:
- Reduce `max-concurrent-file-group-rewrites`
- Increase JVM memory: `export MAVEN_OPTS="-Xmx8g"`
- Process tables in smaller batches

### Performance Monitoring

Monitor these metrics during operations:
- **Files rewritten**: Should be > 0 for effective compaction
- **Bytes rewritten**: Indicates data volume processed
- **Processing time**: Watch for degrading performance
- **Memory usage**: Monitor JVM heap utilization

## Integration Examples

### Automated Maintenance Script

```bash
#!/bin/bash
# Daily maintenance script

echo "Starting daily Iceberg maintenance..."

# Run maintenance on all tables
for table in "demo.employees" "streaming.user_events"; do
    echo "Processing $table..."
    
    # Check if rewrite is needed (pseudo-code)
    echo "rewrite-data $table" | ./run-table-actions.sh
    echo "rewrite-manifests $table" | ./run-table-actions.sh
done

echo "Maintenance completed!"
```

### Monitoring Integration

```java
// Add custom metrics collection
long startTime = System.currentTimeMillis();
RewriteDataFiles.Result result = SparkActions.get()
    .rewriteDataFiles(table)
    .execute();
long duration = System.currentTimeMillis() - startTime;

// Log metrics to monitoring system
logger.info("Rewrite completed: files={}, bytes={}, duration={}ms", 
    result.rewrittenDataFilesCount(), 
    result.rewrittenBytesCount(), 
    duration);
```

## API Reference

### RewriteDataFiles Options

| Option | Default | Description |
|--------|---------|-------------|
| `min-input-files` | `2` | Minimum files required to trigger rewrite |
| `target-file-size-bytes` | `134217728` | Target size for output files (128MB) |
| `max-concurrent-file-group-rewrites` | `4` | Number of parallel rewrite operations |
| `partial-progress.enabled` | `true` | Allow partial completion on errors |

### RewriteManifests Options

| Option | Default | Description |
|--------|---------|-------------|
| `use-caching` | `true` | Enable manifest caching during rewrite |

### ExpireSnapshots Options

| Option | Default | Description |
|--------|---------|-------------|
| `stream-results` | `true` | Stream results for large operations |

## Related Documentation

- [Apache Iceberg Actions API](https://iceberg.apache.org/docs/latest/spark-procedures/)
- [Iceberg Table Maintenance](https://iceberg.apache.org/docs/latest/maintenance/)
- [Spark-Iceberg Integration](https://iceberg.apache.org/docs/latest/spark-configuration/)

---

*This utility provides a workaround for ANTLR conflicts in Spark 4.0 + Iceberg 1.10.0. All operations use the official Iceberg Actions API for production-grade table maintenance.*