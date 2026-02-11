package io.debezium.connector.kingbasees.sync.model;

import lombok.Value;

@Value
public class TableId {
    String schema;
    String table;

    public String asSourceQualified() {
        return quoteSource(schema) + "." + quoteSource(table);
    }

    private static String quoteSource(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    @Override
    public String toString() {
        return schema + "." + table;
    }
}

