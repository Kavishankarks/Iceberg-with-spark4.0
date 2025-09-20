package com.recnos.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

public class KafkaToIcebergStreaming {
    
    private static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String KAFKA_TOPIC = "user-events";
    private static final String CHECKPOINT_LOCATION = "./checkpoint";
    
    public static void main(String[] args) {
        System.out.println("Starting Kafka to Iceberg Streaming Application");
        
        SparkSession spark = createSparkSession();
        
        try {
            // Setup Iceberg table
            setupIcebergTable(spark);
            
            // Start streaming from Kafka to Iceberg
            startKafkaToIcebergStream(spark);
            
        } catch (Exception e) {
            System.err.println("Error in streaming application: " + e.getMessage());
            e.printStackTrace();
        } finally {
            spark.stop();
        }
    }
    
    private static SparkSession createSparkSession() {
        String warehouseDir = System.getProperty("user.dir") + "/warehouse";
        
        return SparkSession.builder()
                .appName("Kafka to Iceberg Streaming")
                .master("local[*]")
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.local.type", "hadoop")
                .config("spark.sql.catalog.local.warehouse", warehouseDir)
                .config("spark.sql.warehouse.dir", warehouseDir)
                .config("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .config("spark.hadoop.fs.defaultFS", "file:///")
                .config("spark.sql.catalogImplementation", "in-memory")
                .getOrCreate();
    }
    
    private static void setupIcebergTable(SparkSession spark) {
        System.out.println("Setting up Iceberg table schema...");
        
        try {
            // Create database
            spark.sql("CREATE DATABASE IF NOT EXISTS local.streaming");
            
            // Drop existing table if it exists
            spark.sql("DROP TABLE IF EXISTS local.streaming.user_events");
            
            // Create Iceberg table with proper schema
            String createTableSQL = """
                CREATE TABLE local.streaming.user_events (
                    event_id BIGINT,
                    timestamp STRING,
                    event_type STRING,
                    user_id BIGINT,
                    user_name STRING,
                    department STRING,
                    location STRING,
                    session_id STRING,
                    ip_address STRING,
                    user_agent STRING,
                    amount DOUBLE,
                    currency STRING,
                    product_id STRING,
                    category STRING,
                    duration_seconds BIGINT,
                    search_term STRING,
                    results_count BIGINT,
                    quantity BIGINT,
                    price DOUBLE,
                    device_type STRING,
                    is_new_user BOOLEAN,
                    page_url STRING,
                    processing_time TIMESTAMP
                ) USING iceberg
                TBLPROPERTIES (
                    'write.format.default' = 'parquet',
                    'write.parquet.compression-codec' = 'snappy'
                )
                """;
            
            spark.sql(createTableSQL);
            System.out.println("Iceberg table created successfully");
            
        } catch (Exception e) {
            System.err.println("Error setting up Iceberg table: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }
    
    private static void startKafkaToIcebergStream(SparkSession spark) throws StreamingQueryException, TimeoutException {
        System.out.println("Starting Kafka to Iceberg streaming...");
        
        // Define the schema for the JSON data
        StructType eventSchema = new StructType()
                .add("event_id", DataTypes.LongType)
                .add("timestamp", DataTypes.StringType)
                .add("event_type", DataTypes.StringType)
                .add("user_id", DataTypes.LongType)
                .add("user_name", DataTypes.StringType)
                .add("department", DataTypes.StringType)
                .add("location", DataTypes.StringType)
                .add("session_id", DataTypes.StringType)
                .add("ip_address", DataTypes.StringType)
                .add("user_agent", DataTypes.StringType)
                .add("amount", DataTypes.DoubleType)
                .add("currency", DataTypes.StringType)
                .add("product_id", DataTypes.StringType)
                .add("category", DataTypes.StringType)
                .add("duration_seconds", DataTypes.LongType)
                .add("search_term", DataTypes.StringType)
                .add("results_count", DataTypes.LongType)
                .add("quantity", DataTypes.LongType)
                .add("price", DataTypes.DoubleType)
                .add("device_type", DataTypes.StringType)
                .add("is_new_user", DataTypes.BooleanType)
                .add("page_url", DataTypes.StringType);
        
        // Read from Kafka
        Dataset<Row> kafkaStream = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
                .option("subscribe", KAFKA_TOPIC)
                .option("startingOffsets", "latest")
                .option("failOnDataLoss", "false")
                .load();
        
        // Parse JSON and add processing timestamp
        Dataset<Row> parsedStream = kafkaStream
                .select(
                    functions.col("key").cast("string").alias("kafka_key"),
                    functions.col("value").cast("string").alias("kafka_value"),
                    functions.col("topic"),
                    functions.col("partition"),
                    functions.col("offset"),
                    functions.col("timestamp").alias("kafka_timestamp")
                )
                .select(
                    functions.col("*"),
                    functions.from_json(functions.col("kafka_value"), eventSchema).alias("event_data")
                )
                .select(
                    functions.col("event_data.*"),
                    functions.current_timestamp().alias("processing_time")
                );
        
        // Write to Iceberg table
        StreamingQuery query = parsedStream
                .writeStream()
                .format("iceberg")
                .outputMode("append")
                .option("path", "local.streaming.user_events")
                .option("checkpointLocation", CHECKPOINT_LOCATION)
                .trigger(Trigger.ProcessingTime("10 seconds"))
                .start();
        
        // Add shutdown hook for graceful termination
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down streaming query...");
            try {
                query.stop();
                System.out.println("Streaming query stopped gracefully");
            } catch (Exception e) {
                System.err.println("Error stopping streaming query: " + e.getMessage());
            }
        }));
        
        System.out.println("Streaming application started successfully!");
        System.out.println("rocessing events from Kafka topic: " + KAFKA_TOPIC);
        System.out.println("Writing to Iceberg table: local.streaming.user_events");
        System.out.println("Trigger interval: 10 seconds");
        System.out.println("Checkpoint location: " + CHECKPOINT_LOCATION);
        System.out.println();
        System.out.println("Press Ctrl+C to stop the application");
        
        // Monitor the streaming query
        monitorStreamingQuery(query, spark);
        
        // Wait for termination
        query.awaitTermination();
    }
    
    private static void monitorStreamingQuery(StreamingQuery query, SparkSession spark) {
        // Start a monitoring thread
        Thread monitorThread = new Thread(() -> {
            try {
                while (query.isActive()) {
                    Thread.sleep(30000); // Check every 30 seconds
                    
                    if (query.lastProgress() != null) {
                        double inputRowsPerSecond = query.lastProgress().inputRowsPerSecond();
                        double processedRowsPerSecond = query.lastProgress().processedRowsPerSecond();
                        long batchDuration = query.lastProgress().batchDuration();
                        
                        System.out.printf("Streaming Stats - Input: %d rows/sec, Processed: %d rows/sec, Batch Duration: %s%n",
                                inputRowsPerSecond, processedRowsPerSecond, batchDuration);
                        
                        // Show recent data from the table
                        try {
                            long count = spark.sql("SELECT COUNT(*) FROM local.streaming.user_events").first().getLong(0);
                            System.out.printf("Total events in Iceberg table: %d%n", count);
                            
                            if (count > 0) {
                                System.out.println("Recent events:");
                                spark.sql("SELECT event_id, event_type, user_name, department, processing_time " +
                                         "FROM local.streaming.user_events " +
                                         "ORDER BY processing_time DESC LIMIT 5").show(5, false);
                            }
                        } catch (Exception e) {
                            System.err.println("Error querying table: " + e.getMessage());
                        }
                    }
                    
                    System.out.println("---");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                System.err.println("Error in monitoring thread: " + e.getMessage());
            }
        });
        
        monitorThread.setDaemon(true);
        monitorThread.start();
    }
}