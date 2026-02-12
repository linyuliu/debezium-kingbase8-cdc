package io.debezium.connector.kingbasees.sink;

import com.alibaba.fastjson2.JSONObject;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Debezium 事件增强转换器：
 * 1) 统一 op/before/after/tombstone 语义
 * 2) 计算 changed_fields
 * 3) 计算数值字段 deltas
 */
final class DebeziumRecordTransformer {

    private final DeltaNullStrategy deltaNullStrategy;
    private final boolean enableChangedFields;
    private final boolean enableDeltas;

    DebeziumRecordTransformer(DeltaNullStrategy deltaNullStrategy, boolean enableChangedFields, boolean enableDeltas) {
        this.deltaNullStrategy = deltaNullStrategy;
        this.enableChangedFields = enableChangedFields;
        this.enableDeltas = enableDeltas;
    }

    EnhancedCdcRecord transform(CdcEvent event, boolean tombstoneAsDelete) {
        if (event == null) {
            return null;
        }

        if (event.isTombstone()) {
            if (!tombstoneAsDelete) {
                return new EnhancedCdcRecord(
                        event.getTableId(),
                        event.getDestination(),
                        "t",
                        true,
                        false,
                        event.getKey(),
                        null,
                        null,
                        null,
                        new ArrayList<String>(),
                        new JSONObject());
            }
            JSONObject keyAsBefore = event.getKey();
            List<String> changedFields = collectChangedFields(keyAsBefore, null);
            return new EnhancedCdcRecord(
                    event.getTableId(),
                    event.getDestination(),
                    "d",
                    true,
                    true,
                    event.getKey(),
                    keyAsBefore,
                    null,
                    keyAsBefore,
                    changedFields,
                    collectDeltas(keyAsBefore, null, changedFields));
        }

        String op = SinkSupport.lower(event.getOp());
        JSONObject before = event.getBefore();
        JSONObject after = event.getAfter();
        boolean deleted = "d".equals(op);
        JSONObject data = deleted ? firstNonNull(before, after) : firstNonNull(after, before);

        List<String> changedFields = collectChangedFields(before, after);
        JSONObject deltas = collectDeltas(before, after, changedFields);

        return new EnhancedCdcRecord(
                event.getTableId(),
                event.getDestination(),
                op,
                false,
                deleted,
                event.getKey(),
                before,
                after,
                data,
                changedFields,
                deltas);
    }

    private List<String> collectChangedFields(JSONObject before, JSONObject after) {
        if (!enableChangedFields) {
            return new ArrayList<String>();
        }

        Set<String> names = new LinkedHashSet<String>();
        if (before != null) {
            names.addAll(before.keySet());
        }
        if (after != null) {
            names.addAll(after.keySet());
        }

        List<String> changed = new ArrayList<String>();
        for (String field : names) {
            Object oldValue = before == null ? null : before.get(field);
            Object newValue = after == null ? null : after.get(field);
            if (!SinkSupport.valuesEqual(oldValue, newValue)) {
                changed.add(field);
            }
        }
        return changed;
    }

    private JSONObject collectDeltas(JSONObject before, JSONObject after, List<String> changedFields) {
        JSONObject deltas = new JSONObject();
        if (!enableDeltas || changedFields == null || changedFields.isEmpty()) {
            return deltas;
        }

        for (String field : changedFields) {
            Object oldValue = before == null ? null : before.get(field);
            Object newValue = after == null ? null : after.get(field);

            BigDecimal oldNum = SinkSupport.toBigDecimal(oldValue);
            BigDecimal newNum = SinkSupport.toBigDecimal(newValue);

            if (oldNum == null || newNum == null) {
                if (deltaNullStrategy != DeltaNullStrategy.ZERO) {
                    continue;
                }
                if (oldNum == null && newNum == null) {
                    continue;
                }
                if (oldNum == null) {
                    oldNum = BigDecimal.ZERO;
                }
                if (newNum == null) {
                    newNum = BigDecimal.ZERO;
                }
            }

            deltas.put(field, newNum.subtract(oldNum));
        }
        return deltas;
    }

    private static JSONObject firstNonNull(JSONObject primary, JSONObject fallback) {
        return primary != null ? primary : fallback;
    }
}
