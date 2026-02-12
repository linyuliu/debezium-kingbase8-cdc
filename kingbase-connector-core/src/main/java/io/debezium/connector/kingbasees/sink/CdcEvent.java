package io.debezium.connector.kingbasees.sink;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import io.debezium.engine.ChangeEvent;

/**
 * Debezium JSON 事件统一解析对象。
 */
final class CdcEvent {

    private final SourceTableId tableId;
    private final String destination;
    private final String op;
    private final JSONObject key;
    private final JSONObject before;
    private final JSONObject after;
    private final boolean tombstone;

    private CdcEvent(SourceTableId tableId,
                     String destination,
                     String op,
                     JSONObject key,
                     JSONObject before,
                     JSONObject after,
                     boolean tombstone) {
        this.tableId = tableId;
        this.destination = destination;
        this.op = op;
        this.key = key;
        this.before = before;
        this.after = after;
        this.tombstone = tombstone;
    }

    static CdcEvent parse(ChangeEvent<String, String> event) {
        if (event == null) {
            return null;
        }

        String destination = event.destination();
        JSONObject keyPayload = parsePayloadObject(event.key());

        // Kafka Tombstone: value 和 valueSchema 可能同时为 null。
        if (event.value() == null || event.value().trim().isEmpty()) {
            return new CdcEvent(parseTableIdFromDestination(destination), destination, "t", keyPayload, null, null, true);
        }

        JSONObject valueRoot = JSON.parseObject(event.value());
        JSONObject payload = payload(valueRoot);
        if (payload == null) {
            return null;
        }

        JSONObject source = payload.getJSONObject("source");
        if (source == null) {
            return null;
        }

        String schema = SinkSupport.text(source, "schema");
        String table = SinkSupport.text(source, "table");
        String op = SinkSupport.text(payload, "op");
        JSONObject before = payload.getJSONObject("before");
        JSONObject after = payload.getJSONObject("after");

        SourceTableId tableId = null;
        if (!SinkSupport.isBlank(schema) && !SinkSupport.isBlank(table)) {
            tableId = new SourceTableId(schema, table);
        }
        if (tableId == null) {
            tableId = parseTableIdFromDestination(destination);
        }
        if (tableId == null) {
            return null;
        }

        return new CdcEvent(tableId, destination, op, keyPayload, before, after, false);
    }

    private static JSONObject parsePayloadObject(String raw) {
        if (SinkSupport.isBlank(raw)) {
            return null;
        }
        try {
            JSONObject root = JSON.parseObject(raw);
            return payload(root);
        }
        catch (Exception ignored) {
            return null;
        }
    }

    private static JSONObject payload(JSONObject root) {
        if (root == null) {
            return null;
        }
        JSONObject payload = root.getJSONObject("payload");
        return payload == null ? root : payload;
    }

    private static SourceTableId parseTableIdFromDestination(String destination) {
        if (SinkSupport.isBlank(destination)) {
            return null;
        }
        String[] parts = destination.split("\\.");
        if (parts.length < 2) {
            return null;
        }
        String schema = parts[parts.length - 2];
        String table = parts[parts.length - 1];
        if (SinkSupport.isBlank(schema) || SinkSupport.isBlank(table)) {
            return null;
        }
        return new SourceTableId(schema, table);
    }

    SourceTableId getTableId() {
        return tableId;
    }

    String getDestination() {
        return destination;
    }

    String getOp() {
        return op;
    }

    JSONObject getKey() {
        return key;
    }

    JSONObject getBefore() {
        return before;
    }

    JSONObject getAfter() {
        return after;
    }

    boolean isTombstone() {
        return tombstone;
    }
}
