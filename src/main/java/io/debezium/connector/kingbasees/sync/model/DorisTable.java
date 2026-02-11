package io.debezium.connector.kingbasees.sync.model;

import lombok.Value;

@Value
public class DorisTable {
    String database;
    String table;

    public String dbQuoted() {
        return quoteDoris(database);
    }

    public String qualified() {
        return quoteDoris(database) + "." + quoteDoris(table);
    }

    private static String quoteDoris(String identifier) {
        return "`" + identifier.replace("`", "``") + "`";
    }

    @Override
    public String toString() {
        return database + "." + table;
    }
}

