package com.recnos.spark;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Scanner;
import java.util.stream.Stream;

public class IcebergMetadataViewer {
    
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static Scanner scanner;
    private static String metadataPath;
    
    public static void main(String[] args) {
        if (args.length > 0) {
            metadataPath = args[0];
        } else {
            metadataPath = System.getProperty("user.dir") + "/warehouse/demo/employees/metadata";
        }
        
        System.out.println("=== Iceberg Metadata Viewer ===");
        System.out.println("Metadata path: " + metadataPath);
        
        scanner = new Scanner(System.in);
        
        try {
            startInteractiveBrowser();
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        } finally {
            if (scanner != null) {
                scanner.close();
            }
        }
    }
    
    private static void startInteractiveBrowser() {
        System.out.println("\n=== Interactive Metadata Browser ===");
        System.out.println("Commands: list, view <file>, latest, schema, snapshots, files, help, exit");
        System.out.println();
        
        while (true) {
            System.out.print("metadata> ");
            String input = scanner.nextLine().trim();
            
            if (input.isEmpty()) {
                continue;
            }
            
            String[] parts = input.split("\\s+", 2);
            String command = parts[0].toLowerCase();
            
            switch (command) {
                case "exit", "quit" -> {
                    System.out.println("Goodbye!");
                    return;
                }
                case "help" -> showHelp();
                case "list" -> listMetadataFiles();
                case "view" -> {
                    if (parts.length > 1) {
                        viewMetadataFile(parts[1]);
                    } else {
                        System.out.println("Usage: view <filename>");
                    }
                }
                case "latest" -> viewLatestMetadata();
                case "schema" -> showSchema();
                case "snapshots" -> showSnapshots();
                case "files" -> showDataFiles();
                default -> System.out.println("Unknown command. Type 'help' for available commands.");
            }
            System.out.println();
        }
    }
    
    private static void showHelp() {
        System.out.println("\n=== Available Commands ===");
        System.out.println("list                 - List all metadata files");
        System.out.println("view <filename>      - View specific metadata file");
        System.out.println("latest               - View latest metadata file");
        System.out.println("schema               - Show table schema");
        System.out.println("snapshots            - Show all snapshots");
        System.out.println("files                - Show data files");
        System.out.println("help                 - Show this help");
        System.out.println("exit/quit            - Exit viewer");
        System.out.println();
    }
    
    private static void listMetadataFiles() {
        try {
            Path path = Paths.get(metadataPath);
            if (!Files.exists(path)) {
                System.out.println("Metadata directory does not exist: " + metadataPath);
                return;
            }
            
            System.out.println("\n=== Metadata Files ===");
            try (Stream<Path> files = Files.list(path)) {
                files.filter(f -> f.toString().endsWith(".metadata.json"))
                     .sorted()
                     .forEach(f -> {
                         try {
                             long size = Files.size(f);
                             String lastModified = LocalDateTime.ofInstant(
                                 Instant.ofEpochMilli(f.toFile().lastModified()), 
                                 ZoneId.systemDefault()
                             ).format(formatter);
                             System.out.printf("%-30s %8d bytes  %s%n", 
                                 f.getFileName(), size, lastModified);
                         } catch (IOException e) {
                             System.out.println(f.getFileName() + " (error reading file info)");
                         }
                     });
            }
        } catch (IOException e) {
            System.err.println("Error listing files: " + e.getMessage());
        }
    }
    
    private static void viewMetadataFile(String filename) {
        try {
            Path filePath = Paths.get(metadataPath, filename);
            if (!Files.exists(filePath)) {
                System.out.println("File not found: " + filename);
                return;
            }
            
            String content = Files.readString(filePath);
            JsonNode metadata = objectMapper.readTree(content);
            
            System.out.println("\n=== Metadata File: " + filename + " ===");
            System.out.println("Format Version: " + metadata.get("format-version").asInt());
            System.out.println("Table UUID: " + metadata.get("table-uuid").asText());
            System.out.println("Location: " + metadata.get("location").asText());
            System.out.println("Last Updated: " + formatTimestamp(metadata.get("last-updated-ms").asLong()));
            
            if (metadata.has("current-snapshot-id")) {
                System.out.println("Current Snapshot ID: " + metadata.get("current-snapshot-id").asLong());
            }
            
            JsonNode properties = metadata.get("properties");
            if (properties != null && properties.size() > 0) {
                System.out.println("\nTable Properties:");
                properties.fields().forEachRemaining(entry -> 
                    System.out.println("  " + entry.getKey() + ": " + entry.getValue().asText())
                );
            }
            
        } catch (IOException e) {
            System.err.println("Error reading file: " + e.getMessage());
        }
    }
    
    private static void viewLatestMetadata() {
        try {
            Path path = Paths.get(metadataPath);
            if (!Files.exists(path)) {
                System.out.println("Metadata directory does not exist: " + metadataPath);
                return;
            }
            
            try (Stream<Path> files = Files.list(path)) {
                files.filter(f -> f.toString().endsWith(".metadata.json"))
                     .max((f1, f2) -> Long.compare(f1.toFile().lastModified(), f2.toFile().lastModified()))
                     .ifPresentOrElse(
                         latest -> viewMetadataFile(latest.getFileName().toString()),
                         () -> System.out.println("No metadata files found")
                     );
            }
        } catch (IOException e) {
            System.err.println("Error finding latest metadata: " + e.getMessage());
        }
    }
    
    private static void showSchema() {
        try {
            JsonNode metadata = getLatestMetadata();
            if (metadata == null) return;
            
            JsonNode currentSchemaId = metadata.get("current-schema-id");
            JsonNode schemas = metadata.get("schemas");
            
            System.out.println("\n=== Table Schema ===");
            System.out.println("Current Schema ID: " + currentSchemaId.asInt());
            
            for (JsonNode schema : schemas) {
                if (schema.get("schema-id").asInt() == currentSchemaId.asInt()) {
                    System.out.println("\nFields:");
                    JsonNode fields = schema.get("fields");
                    for (JsonNode field : fields) {
                        String name = field.get("name").asText();
                        String type = field.get("type").asText();
                        boolean required = field.get("required").asBoolean();
                        int id = field.get("id").asInt();
                        
                        System.out.printf("  %-15s %-10s %s (ID: %d)%n", 
                            name, type, required ? "NOT NULL" : "NULLABLE", id);
                    }
                    break;
                }
            }
        } catch (Exception e) {
            System.err.println("Error reading schema: " + e.getMessage());
        }
    }
    
    private static void showSnapshots() {
        try {
            JsonNode metadata = getLatestMetadata();
            if (metadata == null) return;
            
            JsonNode snapshots = metadata.get("snapshots");
            
            System.out.println("\n=== Snapshots ===");
            System.out.printf("%-20s %-20s %-10s %-15s%n", 
                "Snapshot ID", "Timestamp", "Operation", "Summary");
            System.out.println("-".repeat(70));
            
            for (JsonNode snapshot : snapshots) {
                long snapshotId = snapshot.get("snapshot-id").asLong();
                long timestamp = snapshot.get("timestamp-ms").asLong();
                String operation = snapshot.has("operation") ? 
                    snapshot.get("operation").asText() : "unknown";
                
                JsonNode summary = snapshot.get("summary");
                String summaryText = "";
                if (summary != null && summary.has("added-records")) {
                    summaryText = "+" + summary.get("added-records").asText() + " records";
                }
                
                System.out.printf("%-20s %-20s %-10s %-15s%n", 
                    snapshotId, formatTimestamp(timestamp), operation, summaryText);
            }
        } catch (Exception e) {
            System.err.println("Error reading snapshots: " + e.getMessage());
        }
    }
    
    private static void showDataFiles() {
        try {
            JsonNode metadata = getLatestMetadata();
            if (metadata == null) return;
            
            JsonNode manifests = metadata.get("manifests");
            if (manifests == null || manifests.size() == 0) {
                System.out.println("No manifest files found");
                return;
            }
            
            System.out.println("\n=== Data Files (from manifests) ===");
            System.out.printf("%-30s %-15s %-10s%n", "Manifest File", "Length", "Added Files");
            System.out.println("-".repeat(60));
            
            for (JsonNode manifest : manifests) {
                String manifestPath = manifest.get("manifest-path").asText();
                String filename = Paths.get(manifestPath).getFileName().toString();
                long length = manifest.get("manifest-length").asLong();
                int addedFiles = manifest.get("added-files-count").asInt();
                
                System.out.printf("%-30s %-15d %-10d%n", filename, length, addedFiles);
            }
        } catch (Exception e) {
            System.err.println("Error reading data files: " + e.getMessage());
        }
    }
    
    private static JsonNode getLatestMetadata() throws IOException {
        Path path = Paths.get(metadataPath);
        if (!Files.exists(path)) {
            System.out.println("Metadata directory does not exist: " + metadataPath);
            return null;
        }
        
        try (Stream<Path> files = Files.list(path)) {
            return files.filter(f -> f.toString().endsWith(".metadata.json"))
                       .max((f1, f2) -> Long.compare(f1.toFile().lastModified(), f2.toFile().lastModified()))
                       .map(latest -> {
                           try {
                               String content = Files.readString(latest);
                               return objectMapper.readTree(content);
                           } catch (IOException e) {
                               System.err.println("Error reading latest metadata: " + e.getMessage());
                               return null;
                           }
                       })
                       .orElse(null);
        }
    }
    
    private static String formatTimestamp(long timestampMs) {
        return LocalDateTime.ofInstant(
            Instant.ofEpochMilli(timestampMs), 
            ZoneId.systemDefault()
        ).format(formatter);
    }
}


