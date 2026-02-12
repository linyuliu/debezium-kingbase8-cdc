package io.debezium.connector.kingbasees.console.model;

public enum DataSourceType {
    KINGBASE("KINGBASE", "KingbaseES"),
    DORIS("DORIS", "Doris");

    private final String code;
    private final String label;

    DataSourceType(String code, String label) {
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
