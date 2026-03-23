package io.github.manuranga.dbdump;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class ChangeEventWriter {

    private static final Logger log = LoggerFactory.getLogger(ChangeEventWriter.class);
    private static final DateTimeFormatter FILE_TIME_FMT = DateTimeFormatter.ofPattern("HH-mm-ss.SSS")
            .withZone(ZoneOffset.UTC);
    private static final DateTimeFormatter ISO_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
            .withZone(ZoneOffset.UTC);

    private final ObjectMapper mapper;
    private final Path requestsDir;
    private final Path mainLog;

    public ChangeEventWriter(Path logsDir) throws IOException {
        this.requestsDir = logsDir.resolve("requests");
        this.mainLog = logsDir.resolve("db-dump.txt");
        Files.createDirectories(requestsDir);

        this.mapper = new ObjectMapper();
        this.mapper.enable(SerializationFeature.INDENT_OUTPUT);
    }

    public void handleEvent(String key, String value) throws IOException {
        if (value == null) {
            return; // tombstone
        }

        JsonNode root;
        try {
            root = mapper.readTree(value);
        } catch (JsonProcessingException e) {
            log.warn("Skipping non-JSON event: {}", e.getMessage());
            return;
        }

        JsonNode payload = root.has("payload") ? root.get("payload") : root;

        String op = mapOp(payload.path("op").asText(""));
        if (op.isEmpty()) {
            return; // schema-only or heartbeat event
        }

        JsonNode source = payload.path("source");
        String table = source.path("table").asText("unknown");
        long tsMs = payload.path("ts_ms").asLong(System.currentTimeMillis());
        Instant ts = Instant.ofEpochMilli(tsMs);

        String keyShort = shortKey(key);

        String filename = String.format("%s_db-cdc_%s_%s_%s.txt",
                FILE_TIME_FMT.format(ts), op, table, keyShort);

        JsonNode before = payload.path("before");
        JsonNode after = payload.path("after");

        StringBuilder sb = new StringBuilder();
        sb.append(">>>> CHANGE\n");
        sb.append("Operation: ").append(op).append('\n');
        sb.append("Table: ").append(table).append('\n');
        sb.append("Key: ").append(keyShort).append('\n');
        sb.append("Timestamp: ").append(ISO_FMT.format(ts)).append("\n\n");
        sb.append("Before: ").append(formatNode(before)).append("\n\n");
        sb.append("------------------------\n\n");
        sb.append("After:\n").append(formatNode(after)).append('\n');

        Path file = requestsDir.resolve(filename);
        Files.writeString(file, sb.toString(), StandardCharsets.UTF_8);

        String logLine = filename + " " + op + " " + table + "\n";
        try (Writer w = Files.newBufferedWriter(mainLog, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
            w.write(logLine);
        }

        log.debug("{} {} {}", op, table, keyShort);
    }

    private String mapOp(String op) {
        return switch (op) {
            case "c", "r" -> "INSERT"; // 'r' = snapshot read
            case "u" -> "UPDATE";
            case "d" -> "DELETE";
            default -> "";
        };
    }

    private String shortKey(String key) {
        if (key == null) {
            return "nokey";
        }
        // Try to extract a meaningful short ID from the key JSON
        try {
            JsonNode keyNode = mapper.readTree(key);
            JsonNode payload = keyNode.has("payload") ? keyNode.get("payload") : keyNode;
            // Get first field value as key
            var fields = payload.fields();
            if (fields.hasNext()) {
                String val = fields.next().getValue().asText("");
                if (val.length() > 8) {
                    return val.substring(0, 8);
                }
                return val;
            }
        } catch (Exception ignored) {
        }
        // Fallback: hash of key string
        return Integer.toHexString(key.hashCode() & 0xFFFFFF);
    }

    private String formatNode(JsonNode node) {
        if (node == null || node.isMissingNode() || node.isNull()) {
            return "null";
        }
        try {
            return mapper.writeValueAsString(node);
        } catch (JsonProcessingException e) {
            return node.toString();
        }
    }
}
