package io.debezium.connector.kingbasees.console.model;

public enum OutputMode {
    JDBC_DML("JDBC_DML", "仅 JDBC DML"),
    ENHANCED_JSON_BATCH("ENHANCED_JSON_BATCH", "仅增强 JSON 批量"),
    JDBC_DML_AND_ENHANCED_JSON_BATCH("JDBC_DML_AND_ENHANCED_JSON_BATCH", "JDBC + 增强 JSON");

    private final String code;
    private final String label;

    OutputMode(String code, String label) {
        this.code = code;
        this.label = label;
    }

    public String getCode() {
        return code;
    }

    public String getLabel() {
        return label;
    }
}
