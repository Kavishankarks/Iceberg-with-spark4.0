package com.recnos.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Scanner;

public class testSparkIcebergCLI {
    private static SparkSession spark;
    private static Scanner scanner;

    public static void main(String[] args) {
        System.out.println("Starting test spark Iceberg interactive ");
        System.out.println("Init spark session");

        try {
            initSparkSession();
            setupInitData();

        } catch (Exception e) {
            System.err.println("Failed to init: "+ e.getMessage());
            e.printStackTrace();
        }finally {
            cleanup();
        }
    }

    private static void initSparkSession() {
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

        spark.sparkContext().setLogLevel("ERROR");

        System.out.println("Spark session init successfully");
    }

    private static void setupInitData() {
        System.out.println("Setting up sample db and table");

        spark.sql("create database if not exists local.demo");
        spark.sql("drop table if exists local.demp.employees");

        String createTableSQL = """
                Create table local.demp.employees(
                id BIGINT,
                name string,
                department string
                salary double,
                hire_date DATE,
                ) using iceberg
                partitioned by (department)
                TBLPROPERTIES(
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

        System.out.println("Sample data setup complete");
        System.out.println("Available table: local.demo.employees");
    }

    private static void startInteractiveCLI() {
        scanner = new Scanner(System.in);
        System.out.println("Starting interactive spark shell");
        System.out.println("Enter sql queries (type 'help' for cmds, 'exit' for quit):");

        while (true) {
            System.out.println("spark-sql> ");
            String input = scanner.nextLine().trim();
            if(input.isEmpty()) {
                continue;
            }
            if(input.equalsIgnoreCase("exit") || input.equalsIgnoreCase("quit")) {
                System.out.println("Closing spark session");
                break;
            }

            if(input.equalsIgnoreCase("help")) {
                showHelp();
                continue;
            }
            if (input.equalsIgnoreCase("clear")) {
                clearScreen();
                continue;
            }
            if(input.equalsIgnoreCase("tables")) {
                showTables();
                continue;
            }
            executeQuery(input);
        }
    }

    private static void executeQuery(String sql) {
        long startTime = System.currentTimeMillis();
        if(isSelectQuery(sql)) {
            Dataset<Row> result = spark.sql(sql);
            result.show(20,false);
            long count = result.count();
            long endTime = System.currentTimeMillis();
            System.out.printf("(%d rows, %d ms)%n", count, endTime - startTime);
        } else {
            spark.sql(sql);
            long endTime = System.currentTimeMillis();
            System.out.printf("Query executed successfully (%d ms)%n", endTime - startTime);

        }
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
