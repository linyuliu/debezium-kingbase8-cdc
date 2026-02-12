package io.debezium.connector.kingbasees.sink;

import com.alibaba.fastjson2.JSONObject;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * 增强后的 CDC 记录：
 * 统一承载 op/before/after/key 以及 changed_fields/deltas 等增强信息。
 */
final class EnhancedCdcRecord {

    private final SourceTableId tableId;
    private final String destination;
    private final String op;
    private final boolean tombstone;
    private final boolean deleted;
    private final JSONObject key;
    private final JSONObject before;
    private final JSONObject after;
    private final JSONObject data;
    private final List<String> changedFields;
    private final JSONObject deltas;

    EnhancedCdcRecord(SourceTableId tableId,
                      String destination,
                      String op,
                      boolean tombstone,
                      boolean deleted,
                      JSONObject key,
                      JSONObject before,
                      JSONObject after,
                      JSONObject data,
                      List<String> changedFields,
                      JSONObject deltas) {
        this.tableId = tableId;
        this.destination = destination;
        this.op = op;
        this.tombstone = tombstone;
        this.deleted = deleted;
        this.key = key;
        this.before = before;
        this.after = after;
        this.data = data;
        this.changedFields = changedFields == null ? Collections.<String>emptyList() : changedFields;
        this.deltas = deltas == null ? new JSONObject() : deltas;
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

    boolean isTombstone() {
        return tombstone;
    }

    boolean isDeleted() {
        return deleted;
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

    JSONObject getData() {
        return data;
    }

    List<String> getChangedFields() {
        return changedFields;
    }

    JSONObject getDeltas() {
        return deltas;
    }

    JSONObject toEnhancedJson() {
        JSONObject out = new JSONObject();
        if (data != null) {
            for (Map.Entry<String, Object> entry : data.entrySet()) {
                out.put(entry.getKey(), entry.getValue());
            }
        }
        out.put("__op", op);
        out.put("__deleted", deleted);
        out.put("__tombstone", tombstone);
        if (!changedFields.isEmpty()) {
            out.put("changed_fields", changedFields);
        }
        if (deltas != null && !deltas.isEmpty()) {
            out.put("deltas", deltas);
        }
        if (tableId != null) {
            out.put("__table", tableId.toString());
        }
        if (!SinkSupport.isBlank(destination)) {
            out.put("__destination", destination);
        }
        if (key != null && !key.isEmpty()) {
            out.put("__key", key);
        }
        return out;
    }
}
