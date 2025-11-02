package com.recnos.spark;

import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.actions.RewriteDataFilesActionResult;
import org.apache.iceberg.actions.RewriteManifests;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.spark.sql.SparkSession;
import org.apache.iceberg.spark.actions.SparkActions;

import java.util.Scanner;

public class IcebergTableActions {
    
    private static final String WAREHOUSE_DIR = System.getProperty("user.dir") + "/warehouse";
    
    public static void main(String[] args) {
        System.out.println("🔧 Iceberg Table Actions Utility");
        
        SparkSession spark = createSparkSession();
        
        try {
            showAvailableActions();
            runInteractiveActions(spark);
            
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            spark.stop();
        }
    }
    
    private static SparkSession createSparkSession() {
        return SparkSession.builder()
                .appName("Iceberg Table Actions")
                .master("local[*]")
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.local.type", "hadoop")
                .config("spark.sql.catalog.local.warehouse", WAREHOUSE_DIR)
                .config("spark.sql.warehouse.dir", WAREHOUSE_DIR)
                .config("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .config("spark.hadoop.fs.defaultFS", "file:///")
                .config("spark.sql.catalogImplementation", "in-memory")
                .getOrCreate();
    }
    
    private static void showAvailableActions() {
        System.out.println("\n📋 Available Iceberg Actions:");
        System.out.println("1. rewrite-data - Rewrite data files for compaction");
        System.out.println("2. rewrite-manifests - Rewrite manifest files");
        System.out.println("3. expire-snapshots - Remove old snapshots");
        System.out.println("4. remove-orphan-files - Clean up orphaned files");
        System.out.println("5. show-table-info - Display table information");
        System.out.println("6. list-tables - List available tables in warehouse");
        System.out.println("7. exit - Exit the application");
        System.out.println();
        
        // Show available tables on startup
        listAvailableTables();
    }
    
    private static void runInteractiveActions(SparkSession spark) {
        Scanner scanner = new Scanner(System.in);
        
        while (true) {
            System.out.print("iceberg-actions> ");
            String input = scanner.nextLine().trim();
            
            if (input.equalsIgnoreCase("exit")) {
                break;
            }
            
            try {
                executeAction(spark, input);
            } catch (Exception e) {
                System.err.println("Error executing action: " + e.getMessage());
            }
        }
        
        scanner.close();
    }
    
    private static void executeAction(SparkSession spark, String input) {
        String[] parts = input.split("\\s+");
        String action = parts[0];
        
        // Handle actions that don't need a table name
        if (action.equalsIgnoreCase("list-tables")) {
            listAvailableTables();
            return;
        }
        
        if (parts.length < 2) {
            System.out.println("Usage: <action> <table_name> [options]");
            System.out.println("Example: rewrite-data demo.employees");
            System.out.println("         show-table-info demo.employees");
            return;
        }
        
        String tableName = parts[1];
        
        try {
            Table table = loadTable(tableName);
            
            switch (action.toLowerCase()) {
                case "rewrite-data":
                    rewriteDataFiles(table);
                    break;
                case "rewrite-manifests":
                    rewriteManifests(table);
                    break;
                case "expire-snapshots":
                    expireSnapshots(table);
                    break;
                case "remove-orphan-files":
                    removeOrphanFiles(table);
                    break;
                case "show-table-info":
                    showTableInfo(spark, tableName);
                    break;
                default:
                    System.out.println("Unknown action: " + action);
                    showAvailableActions();
            }
        } catch (Exception e) {
            System.err.println("Failed to execute action '" + action + "' on table '" + tableName + "': " + e.getMessage());
        }
    }
    
    private static Table loadTable(String tableName) {
        try {
            // Create Hadoop catalog with proper configuration
            HadoopCatalog catalog = new HadoopCatalog();
            org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
            hadoopConf.set("fs.defaultFS", "file:///");
            catalog.setConf(hadoopConf);
            catalog.initialize("local", java.util.Map.of("warehouse", WAREHOUSE_DIR));
            
            // Parse table identifier - handle both formats
            TableIdentifier tableId;
            if (tableName.startsWith("local.")) {
                // Remove the catalog prefix for Hadoop catalog
                String tableNameWithoutCatalog = tableName.substring(6); // Remove "local."
                tableId = TableIdentifier.parse(tableNameWithoutCatalog);
            } else {
                tableId = TableIdentifier.parse(tableName);
            }
            
            System.out.println("🔍 Loading table: " + tableId + " from warehouse: " + WAREHOUSE_DIR);
            
            return catalog.loadTable(tableId);
        } catch (Exception e) {
            System.err.println("Error loading table '" + tableName + "': " + e.getMessage());
            throw new RuntimeException("Failed to load table: " + tableName, e);
        }
    }
    
    private static void rewriteDataFiles(Table table) {
        System.out.println("🔄 Starting data files rewrite...");
        
        try {
            RewriteDataFiles.Result result = SparkActions.get()
                    .rewriteDataFiles(table)
                    .option("min-input-files", "2")
                    .option("target-file-size-bytes", "134217728") // 128MB
                    .option("max-concurrent-file-group-rewrites", "4")
                    .option("partial-progress.enabled", "true")
                    .execute();
            
            System.out.println("✅ Data files rewrite completed!");
            System.out.println("Results:");
            System.out.println("   • Files rewritten: " + result.rewrittenDataFilesCount());
            System.out.println("   • Files added: " + result.addedDataFilesCount());
            System.out.println("   • Bytes rewritten: " + formatBytes(result.rewrittenBytesCount()));
            System.out.println("   • Files groups: " + result.rewriteResults().size());
            
        } catch (Exception e) {
            System.err.println("Data files rewrite failed: " + e.getMessage());
        }
    }
    
    private static void rewriteManifests(Table table) {
        System.out.println("🔄 Starting manifest rewrite...");
        
        try {
            RewriteManifests.Result result = SparkActions.get()
                    .rewriteManifests(table)
                    .option("use-caching", "true")
                    .execute();
            
            System.out.println("✅ Manifest rewrite completed!");
            System.out.println("Results:");
            System.out.println("   • Manifests added: " + result.addedManifests());
            
        } catch (Exception e) {
            System.err.println("Manifest rewrite failed: " + e.getMessage());
        }
    }
    
    private static void expireSnapshots(Table table) {
        System.out.println("🔄 Starting snapshot expiration...");
        
        try {
            // Expire snapshots older than 7 days
            long expireTimestamp = System.currentTimeMillis() - (7L * 24 * 60 * 60 * 1000);
            
            SparkActions.get()
                    .expireSnapshots(table)
                    .expireOlderThan(expireTimestamp)
                    .option("stream-results", "true")
                    .execute();
            
            System.out.println("✅ Snapshot expiration completed!");
            
        } catch (Exception e) {
            System.err.println("Snapshot expiration failed: " + e.getMessage());
        }
    }
    
    private static void removeOrphanFiles(Table table) {
        System.out.println("🔄 Starting orphan file removal...");
        
        try {
            // Remove files older than 3 days that are not referenced
            long olderThan = System.currentTimeMillis() - (3L * 24 * 60 * 60 * 1000);
            
            var result = SparkActions.get()
                    .deleteOrphanFiles(table)
                    .olderThan(olderThan)
                    .execute();
            
            System.out.println("✅ Orphan file removal completed!");
            System.out.println("Results:");
            System.out.println("   • Orphan files found: " + result.orphanFileLocations());
            
        } catch (Exception e) {
            System.err.println("Orphan file removal failed: " + e.getMessage());
        }
    }
    
    private static void showTableInfo(SparkSession spark, String tableName) {
        System.out.println("📋 Table Information for: " + tableName);
        
        try {
            // Show table schema
            System.out.println("\n🏗️  Schema:");
            spark.sql("DESCRIBE " + tableName).show(false);

            // Show table properties
            System.out.println("\n⚙️  Properties:");
            spark.sql("SHOW TBLPROPERTIES " + tableName).show(false);
            
            // Show snapshots
            System.out.println("\n📸 Snapshots:");
            spark.sql("SELECT snapshot_id, committed_at, summary FROM " + tableName + ".snapshots ORDER BY committed_at DESC LIMIT 5").show(false);
            
            // Show files
            System.out.println("\n📁 Data Files:");
            spark.sql("SELECT file_path, file_format, record_count, file_size_in_bytes FROM " + tableName + ".files LIMIT 10").show(false);
            
        } catch (Exception e) {
            System.err.println("Failed to show table info: " + e.getMessage());
        }
    }
    
    private static void listAvailableTables() {
        System.out.println("📚 Available Tables in Warehouse:");
        
        try {
            java.io.File warehouseDir = new java.io.File(WAREHOUSE_DIR);
            if (!warehouseDir.exists()) {
                System.out.println("   Warehouse directory not found: " + WAREHOUSE_DIR);
                return;
            }
            
            // Look for database directories
            java.io.File[] databases = warehouseDir.listFiles(java.io.File::isDirectory);
            if (databases == null || databases.length == 0) {
                System.out.println("   📭 No databases found in warehouse");
                return;
            }
            
            for (java.io.File dbDir : databases) {
                System.out.println("   📂 Database: " + dbDir.getName());
                
                // Look for table directories in each database
                java.io.File[] tables = dbDir.listFiles(java.io.File::isDirectory);
                if (tables != null) {
                    for (java.io.File tableDir : tables) {
                        // Check if it's actually an Iceberg table by looking for metadata
                        java.io.File metadataDir = new java.io.File(tableDir, "metadata");
                        if (metadataDir.exists()) {
                            System.out.println("      📋 Table: " + dbDir.getName() + "." + tableDir.getName());
                        }
                    }
                }
            }
            
        } catch (Exception e) {
            System.err.println("Error listing tables: " + e.getMessage());
        }
        
        System.out.println();
    }
    
    private static String formatBytes(long bytes) {
        if (bytes < 1024) return bytes + " B";
        int exp = (int) (Math.log(bytes) / Math.log(1024));
        String pre = "KMGTPE".charAt(exp - 1) + "";
        return String.format("%.1f %sB", bytes / Math.pow(1024, exp), pre);
    }
}