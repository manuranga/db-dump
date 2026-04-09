package io.github.manuranga.dbdump;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DbDumpApp {

    private static final Logger log = LoggerFactory.getLogger(DbDumpApp.class);

    public static void main(String[] args) throws IOException {
        String dbHost = env("DB_HOST", "localhost");
        String dbPort = env("DB_PORT", "5432");
        String dbName = requireEnv("DB_NAME");
        String dbUser = requireEnv("DB_USER");
        String dbPassword = requireEnv("DB_PASSWORD");
        String offsetFile = env("OFFSET_FILE", "data/offsets.dat");
        String requestsDir = env("LOGS_DIR", "logs");
        String mainLog = env("MAIN_LOG", "");
        String slotName = env("SLOT_NAME", "debezium");
        String topicPrefix = env("TOPIC_PREFIX", "dbdump");

        List<String> errors = new ArrayList<>();
        try {
            int port = Integer.parseInt(dbPort);
            if (port < 1 || port > 65535) {
                errors.add("DB_PORT must be between 1 and 65535, got: " + dbPort);
            }
        } catch (NumberFormatException e) {
            errors.add("DB_PORT must be a valid integer, got: " + dbPort);
        }
        if (dbHost.isBlank()) {
            errors.add("DB_HOST must not be blank");
        }
        if (!errors.isEmpty()) {
            errors.forEach(err -> log.error("Configuration error: {}", err));
            System.exit(1);
        }

        Path dataDir = Path.of(offsetFile).getParent();
        if (dataDir != null) {
            Files.createDirectories(dataDir);
        }

        ChangeEventWriter writer = new ChangeEventWriter(
                Path.of(requestsDir),
                mainLog.isBlank() ? null : Path.of(mainLog));

        Properties props = new Properties();
        props.setProperty("name", "db-dump");
        props.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector");

        props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
        props.setProperty("offset.storage.file.filename", offsetFile);
        props.setProperty("offset.flush.interval.ms", "5000");

        props.setProperty("database.hostname", dbHost);
        props.setProperty("database.port", dbPort);
        props.setProperty("database.user", dbUser);
        props.setProperty("database.password", dbPassword);
        props.setProperty("database.dbname", dbName);
        props.setProperty("topic.prefix", topicPrefix);
        props.setProperty("slot.name", slotName);
        props.setProperty("plugin.name", "pgoutput");
        props.setProperty("snapshot.mode", "initial");
        props.setProperty("schema.include.list", "public");
        props.setProperty("decimal.handling.mode", "string");
        props.setProperty("publication.autocreate.mode", "all_tables");

        // Tombstone and schema settings
        props.setProperty("tombstones.on.delete", "false");
        props.setProperty("key.converter.schemas.enable", "false");
        props.setProperty("value.converter.schemas.enable", "false");

        log.info("Starting db-dump CDC capture: {}:{}/{}", dbHost, dbPort, dbName);

        DebeziumEngine<ChangeEvent<String, String>> engine = DebeziumEngine.create(Json.class)
                .using(props)
                .notifying(record -> {
                    try {
                        writer.handleEvent(record.key(), record.value());
                    } catch (Exception e) {
                        log.error("Failed to process CDC event", e);
                    }
                })
                .using((success, message, error) -> {
                    if (!success) {
                        log.error("Debezium engine stopped with error: {}", message, error);
                    } else {
                        log.info("Debezium engine stopped: {}", message);
                    }
                })
                .build();

        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.execute(engine);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down db-dump...");
            try {
                engine.close();
            } catch (IOException e) {
                log.error("Error closing engine", e);
            }
            executor.shutdown();
        }));

        log.info("db-dump is running. Waiting for CDC events...");
    }

    private static String requireEnv(String key) {
        String val = System.getenv(key);
        if (val == null || val.isBlank()) {
            log.error("Required environment variable {} is not set", key);
            System.exit(1);
        }
        return val;
    }

    private static String env(String key, String defaultValue) {
        String val = System.getenv(key);
        return val != null ? val : defaultValue;
    }
}
