package io.debezium.connector.kingbasees.sink;

import java.util.Objects;

/**
 * 源端表标识（schema.table）。
 */
final class SourceTableId {

    private final String schema;
    private final String table;

    SourceTableId(String schema, String table) {
        this.schema = schema;
        this.table = table;
    }

    String getSchema() {
        return schema;
    }

    String getTable() {
        return table;
    }

    String toQuotedName() {
        return SinkSupport.quoteIdentifier(schema) + "." + SinkSupport.quoteIdentifier(table);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SourceTableId)) {
            return false;
        }
        SourceTableId that = (SourceTableId) o;
        return Objects.equals(schema, that.schema) && Objects.equals(table, that.table);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema, table);
    }

    @Override
    public String toString() {
        return schema + "." + table;
    }
}
