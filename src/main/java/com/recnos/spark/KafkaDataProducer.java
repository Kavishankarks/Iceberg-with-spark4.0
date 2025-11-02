package com.recnos.spark;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class KafkaDataProducer {
    
    private static final String TOPIC_NAME = "user-events";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    // Sample data for generating realistic events
    private static final String[] DEPARTMENTS = {"Engineering", "Marketing", "Sales", "HR", "Finance", "Operations"};
    private static final String[] ACTIONS = {"login", "logout", "purchase", "view_product", "add_to_cart", "search", "profile_update"};
    private static final String[] LOCATIONS = {"New York", "San Francisco", "London", "Tokyo", "Sydney", "Mumbai", "Berlin"};
    private static final String[] FIRST_NAMES = {"John", "Jane", "Bob", "Alice", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry"};
    private static final String[] LAST_NAMES = {"Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"};
    
    public static void main(String[] args) {
        System.out.println("Starting Kafka Data Producer for Iceberg Integration");
        
        // Parse arguments
        int numEvents = args.length > 0 ? Integer.parseInt(args[0]) : 1000;
        int intervalMs = args.length > 1 ? Integer.parseInt(args[1]) : 100;
        
        System.out.printf("Generating %d events with %dms interval%n", numEvents, intervalMs);
        
        KafkaDataProducer producer = new KafkaDataProducer();
        producer.generateAndSendEvents(numEvents, intervalMs);
    }
    
    private void generateAndSendEvents(int numEvents, int intervalMs) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            
            for (int i = 0; i < numEvents; i++) {
                try {
                    // Generate event data
                    Map<String, Object> event = generateUserEvent(i + 1);
                    String eventJson = objectMapper.writeValueAsString(event);
                    String key = String.valueOf(event.get("user_id"));
                    
                    // Send to Kafka
                    ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, key, eventJson);
                    int finalI = i;
                    producer.send(record, (metadata, exception) -> {
                        if (exception == null) {
                            if ((finalI + 1) % 100 == 0) {
                                System.out.printf("Sent %d events to topic %s%n", finalI + 1, TOPIC_NAME);
                            }
                        } else {
                            System.err.printf("Error sending event %d: %s%n", finalI + 1, exception.getMessage());
                        }
                    });
                    
                    // Wait between events
                    if (intervalMs > 0) {
                        Thread.sleep(intervalMs);
                    }
                    
                } catch (Exception e) {
                    System.err.printf("Error generating event %d: %s%n", i + 1, e.getMessage());
                }
            }
            
            // Flush and close
            producer.flush();
            System.out.println("Finished sending all events");
            
        } catch (Exception e) {
            System.err.println("Error with Kafka producer: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private Map<String, Object> generateUserEvent(int eventId) {
        Map<String, Object> event = new HashMap<>();
        Random random = ThreadLocalRandom.current();
        
        // Basic event information
        event.put("event_id", eventId);
        event.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        event.put("event_type", ACTIONS[random.nextInt(ACTIONS.length)]);
        
        // User information
        int userId = random.nextInt(1000) + 1;
        event.put("user_id", userId);
        event.put("user_name", FIRST_NAMES[random.nextInt(FIRST_NAMES.length)] + " " + 
                               LAST_NAMES[random.nextInt(LAST_NAMES.length)]);
        event.put("department", DEPARTMENTS[random.nextInt(DEPARTMENTS.length)]);
        event.put("location", LOCATIONS[random.nextInt(LOCATIONS.length)]);
        
        // Event details
        event.put("session_id", "session_" + UUID.randomUUID().toString().substring(0, 8));
        event.put("ip_address", generateRandomIP());
        event.put("user_agent", "Mozilla/5.0 (compatible; DataGenerator/1.0)");
        
        // Add event-specific data based on action type
        String eventType = (String) event.get("event_type");
        switch (eventType) {
            case "purchase":
                event.put("amount", random.nextDouble() * 1000);
                event.put("currency", "USD");
                event.put("product_id", "product_" + random.nextInt(100));
                break;
            case "view_product":
                event.put("product_id", "product_" + random.nextInt(100));
                event.put("category", "electronics");
                event.put("duration_seconds", random.nextInt(300) + 10);
                break;
            case "search":
                event.put("search_term", "search_query_" + random.nextInt(50));
                event.put("results_count", random.nextInt(100));
                break;
            case "add_to_cart":
                event.put("product_id", "product_" + random.nextInt(100));
                event.put("quantity", random.nextInt(5) + 1);
                event.put("price", random.nextDouble() * 100);
                break;
        }
        
        // Add some random metadata
        event.put("device_type", random.nextBoolean() ? "mobile" : "desktop");
        event.put("is_new_user", random.nextBoolean());
        event.put("page_url", "/page/" + random.nextInt(20));
        
        return event;
    }
    
    private String generateRandomIP() {
        Random random = ThreadLocalRandom.current();
        return String.format("%d.%d.%d.%d",
                random.nextInt(256),
                random.nextInt(256),
                random.nextInt(256),
                random.nextInt(256));
    }
}