package com.recnos.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Scanner;

public class SparkIcebergCLI {
    
    private static SparkSession spark;
    private static Scanner scanner;
    
    public static void main(String[] args) {
        System.out.println("=== Spark Iceberg Interactive CLI ===");
        System.out.println("Initializing Spark session...");
        
        try {
            initializeSparkSession();
            setupInitialData();
            startInteractiveCLI();
        } catch (Exception e) {
            System.err.println("Failed to initialize: " + e.getMessage());
        } finally {
            cleanup();
        }
    }
    
    private static void initializeSparkSession() {
        String warehouseDir = System.getProperty("user.dir") + "/warehouse";
        
        spark = SparkSession.builder()
                .appName("Spark Iceberg CLI")
                .master("local[*]")
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.local.type", "hadoop")
                .config("spark.sql.catalog.local.warehouse", warehouseDir)
                .config("spark.sql.warehouse.dir", warehouseDir)
                .config("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.hadoop.fs.defaultFS", "file:///")
                .config("spark.sql.catalogImplementation", "in-memory")
                .config("spark.kryo.registrationRequired", "false")
                .config("spark.kryo.unsafe", "false")
                .getOrCreate();
        
        // Set log levels programmatically to suppress verbose logging
        spark.sparkContext().setLogLevel("ERROR");
        
        System.out.println("✓ Spark session initialized successfully");
    }
    
    private static void setupInitialData() {
        try {
            System.out.println("Setting up sample database and table...");
            
            spark.sql("CREATE DATABASE IF NOT EXISTS local.demo");
            
            spark.sql("DROP TABLE IF EXISTS local.demo.employees");
            
            String createTableSQL = """
                CREATE TABLE local.demo.employees (
                    id BIGINT,
                    name STRING,
                    department STRING,
                    salary DOUBLE,
                    hire_date DATE
                ) USING iceberg
                PARTITIONED BY (department)
                TBLPROPERTIES (
                    'write.format.default' = 'parquet',
                    'write.parquet.compression-codec' = 'snappy'
                )
                """;
            
            spark.sql(createTableSQL);
            
            String insertSQL = """
                INSERT INTO local.demo.employees VALUES
                (1, 'John Doe', 'Engineering', 75000.0, DATE('2023-01-15')),
                (2, 'Jane Smith', 'Marketing', 65000.0, DATE('2023-02-20')),
                (3, 'Bob Johnson', 'Engineering', 80000.0, DATE('2023-03-10')),
                (4, 'Alice Brown', 'Sales', 70000.0, DATE('2023-04-05')),
                (5, 'Charlie Wilson', 'Engineering', 85000.0, DATE('2023-05-12'))
                """;
            
            spark.sql(insertSQL);
            
            System.out.println("✓ Sample data setup complete");
            System.out.println("Available table: local.demo.employees");
            
        } catch (Exception e) {
            System.err.println("Error setting up initial data: " + e.getMessage());
        }
    }
    
    private static void startInteractiveCLI() {
        scanner = new Scanner(System.in);
        
        System.out.println("\n=== Interactive Spark SQL CLI ===");
        System.out.println("Enter SQL queries (type 'help' for commands, 'exit' to quit):");
        System.out.println();
        
        while (true) {
            System.out.print("spark-sql> ");
            String input = scanner.nextLine().trim();
            
            if (input.isEmpty()) {
                continue;
            }
            
            if (input.equalsIgnoreCase("exit") || input.equalsIgnoreCase("quit")) {
                System.out.println("Goodbye!");
                break;
            }
            
            if (input.equalsIgnoreCase("help")) {
                showHelp();
                continue;
            }
            
            if (input.equalsIgnoreCase("clear")) {
                clearScreen();
                continue;
            }
            
            if (input.equalsIgnoreCase("tables")) {
                showTables();
                continue;
            }
            
            if (input.startsWith("desc ") || input.startsWith("describe ")) {
                executeQuery(input);
                continue;
            }
            
            executeQuery(input);
        }
    }
    
    private static void executeQuery(String sql) {
        try {
            long startTime = System.currentTimeMillis();
            
            if (isSelectQuery(sql)) {
                Dataset<Row> result = spark.sql(sql);
                result.show(30, false);
                
                long count = result.count();
                long endTime = System.currentTimeMillis();
                System.out.println(String.format("(%d rows, %d ms)", count, endTime - startTime));
                
            } else {
                spark.sql(sql);
                long endTime = System.currentTimeMillis();
                System.out.println(String.format("Query executed successfully (%d ms)", endTime - startTime));
            }
            
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            
            if (e.getCause() != null && e.getCause().getMessage() != null) {
                System.err.println("Cause: " + e.getCause().getMessage());
            }
        }
        System.out.println();
    }
    
    private static boolean isSelectQuery(String sql) {
        String trimmed = sql.trim().toLowerCase();
        return trimmed.startsWith("select") || 
               trimmed.startsWith("show") || 
               trimmed.startsWith("describe") || 
               trimmed.startsWith("desc") ||
               trimmed.startsWith("explain");
    }
    
    private static void showHelp() {
        System.out.println("\n=== Available Commands ===");
        System.out.println("help          - Show this help message");
        System.out.println("exit/quit     - Exit the CLI");
        System.out.println("clear         - Clear the screen");
        System.out.println("tables        - List available tables");
        System.out.println();
        System.out.println("=== Sample Queries ===");
        System.out.println("SELECT * FROM local.demo.employees;");
        System.out.println("SELECT department, COUNT(*) FROM local.demo.employees GROUP BY department;");
        System.out.println("DESCRIBE local.demo.employees;");
        System.out.println("SHOW TBLPROPERTIES local.demo.employees;");
        System.out.println("SELECT * FROM local.demo.employees.snapshots;");
        System.out.println();
        System.out.println("=== Iceberg Commands ===");
        System.out.println("-- Time travel");
        System.out.println("SELECT * FROM local.demo.employees TIMESTAMP AS OF '2023-01-01';");
        System.out.println("-- Schema evolution");
        System.out.println("ALTER TABLE local.demo.employees ADD COLUMN email STRING;");
        System.out.println("-- Insert data");
        System.out.println("INSERT INTO local.demo.employees VALUES (6, 'New Employee', 'HR', 60000.0, DATE('2023-08-01'));");
        System.out.println();
    }
    
    private static void showTables() {
        try {
            System.out.println("\n=== Available Tables ===");
            spark.sql("SHOW TABLES FROM local.demo").show(false);
        } catch (Exception e) {
            System.err.println("Error listing tables: " + e.getMessage());
        }
        System.out.println();
    }
    
    private static void clearScreen() {
        try {
            String os = System.getProperty("os.name").toLowerCase();
            if (os.contains("windows")) {
                new ProcessBuilder("cmd", "/c", "cls").inheritIO().start().waitFor();
            } else {
                System.out.print("\033[2J\033[H");
                System.out.flush();
            }
        } catch (Exception e) {
            for (int i = 0; i < 50; i++) {
                System.out.println();
            }
        }
    }
    
    private static void cleanup() {
        if (scanner != null) {
            scanner.close();
        }
        if (spark != null) {
            spark.stop();
        }
    }
}