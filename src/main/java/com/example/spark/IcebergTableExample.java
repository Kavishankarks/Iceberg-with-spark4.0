package com.example.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

public class IcebergTableExample {
    
    public static void main(String[] args) {
        SparkSession spark = createSparkSession();
        
        try {
            demonstrateIcebergOperations(spark);
        } finally {
            spark.stop();
        }
    }
    
    private static SparkSession createSparkSession() {
        return SparkSession.builder()
                .appName("Iceberg Table Example")
                .master("local[*]")
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
                .config("spark.sql.catalog.spark_catalog.type", "hive")
                .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.local.type", "hadoop")
                .config("spark.sql.catalog.local.warehouse", "warehouse")
                .config("spark.sql.warehouse.dir", "warehouse")
                .getOrCreate();
    }
    
    private static void demonstrateIcebergOperations(SparkSession spark) {
        System.out.println("=== Iceberg Table Creation and Management Demo ===\n");
        
        // 1. Create Iceberg table
        createIcebergTable(spark);
        
        // 2. Insert data
        insertData(spark);
        
        // 3. Query data
        queryTable(spark);
        
        // 4. Show table metadata
        showTableMetadata(spark);
        
        // 5. Demonstrate time travel
        demonstrateTimeTravel(spark);
        
        // 6. Schema evolution
        demonstrateSchemaEvolution(spark);
    }
    
    private static void createIcebergTable(SparkSession spark) {
        System.out.println("1. Creating Iceberg table...");
        
        spark.sql("DROP TABLE IF EXISTS local.db.employees");
        spark.sql("CREATE DATABASE IF NOT EXISTS local.db");
        
        String createTableSQL = """
            CREATE TABLE local.db.employees (
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
        System.out.println("✓ Iceberg table 'employees' created successfully\n");
    }
    
    private static void insertData(SparkSession spark) {
        System.out.println("2. Inserting sample data...");
        
        String insertSQL = """
            INSERT INTO local.db.employees VALUES
            (1, 'John Doe', 'Engineering', 75000.0, '2023-01-15'),
            (2, 'Jane Smith', 'Marketing', 65000.0, '2023-02-20'),
            (3, 'Bob Johnson', 'Engineering', 80000.0, '2023-03-10'),
            (4, 'Alice Brown', 'Sales', 70000.0, '2023-04-05'),
            (5, 'Charlie Wilson', 'Engineering', 85000.0, '2023-05-12')
            """;
        
        spark.sql(insertSQL);
        System.out.println("✓ Sample data inserted successfully\n");
    }
    
    private static void queryTable(SparkSession spark) {
        System.out.println("3. Querying Iceberg table...");
        
        Dataset<Row> result = spark.sql("SELECT * FROM local.db.employees ORDER BY id");
        result.show();
        
        System.out.println("Query by department:");
        spark.sql("SELECT department, COUNT(*) as employee_count, AVG(salary) as avg_salary " +
                 "FROM local.db.employees GROUP BY department").show();
        System.out.println();
    }
    
    private static void showTableMetadata(SparkSession spark) {
        System.out.println("4. Table metadata and properties...");
        
        System.out.println("Table schema:");
        spark.sql("DESCRIBE local.db.employees").show();
        
        System.out.println("Table properties:");
        spark.sql("SHOW TBLPROPERTIES local.db.employees").show();
        
        System.out.println("Table partitions:");
        spark.sql("SHOW PARTITIONS local.db.employees").show();
        System.out.println();
    }
    
    private static void demonstrateTimeTravel(SparkSession spark) {
        System.out.println("5. Demonstrating time travel...");
        
        System.out.println("Current snapshots:");
        spark.sql("SELECT snapshot_id, committed_at, summary " +
                 "FROM local.db.employees.snapshots ORDER BY committed_at").show(false);
        
        // Add more data to create another snapshot
        spark.sql("INSERT INTO local.db.employees VALUES " +
                 "(6, 'David Lee', 'Marketing', 72000.0, '2023-06-01')");
        
        System.out.println("After adding new data - snapshots:");
        spark.sql("SELECT snapshot_id, committed_at, summary " +
                 "FROM local.db.employees.snapshots ORDER BY committed_at").show(false);
        System.out.println();
    }
    
    private static void demonstrateSchemaEvolution(SparkSession spark) {
        System.out.println("6. Demonstrating schema evolution...");
        
        System.out.println("Adding new column 'email':");
        spark.sql("ALTER TABLE local.db.employees ADD COLUMN email STRING");
        
        System.out.println("Updated schema:");
        spark.sql("DESCRIBE local.db.employees").show();
        
        // Insert data with new column
        spark.sql("INSERT INTO local.db.employees VALUES " +
                 "(7, 'Emma Davis', 'HR', 68000.0, '2023-07-01', 'emma.davis@company.com')");


        System.out.println("Data after schema evolution:");
        spark.sql("SELECT * FROM local.db.employees WHERE id >= 6").show();
        System.out.println();
    }
}