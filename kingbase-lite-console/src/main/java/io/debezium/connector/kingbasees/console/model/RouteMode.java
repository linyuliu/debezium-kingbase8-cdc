package io.debezium.connector.kingbasees.console.model;

/**
 * 目标路由模式。
 */
public enum RouteMode {
    SCHEMA_TABLE("schema_table", "同库分表(schema__table)"),
    SCHEMA_AS_DB("schema_as_db", "按 schema 分库");

    private final String code;
    private final String label;

    RouteMode(String code, String label) {
        this.code = code;
        this.label = label;
    }

    public String getCode() {
        return code;
    }

    public String getLabel() {
        return label;
    }

    public static RouteMode fromCode(String code) {
        if (code == null || code.trim().isEmpty()) {
            return SCHEMA_TABLE;
        }
        String value = code.trim();
        for (RouteMode mode : values()) {
            if (mode.code.equalsIgnoreCase(value)) {
                return mode;
            }
        }
        throw new IllegalArgumentException("不支持的路由模式: " + code);
    }
}
