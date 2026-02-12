package io.debezium.connector.kingbasees.console.model;

public enum DeleteSyncMode {
    PHYSICAL_DELETE("PHYSICAL_DELETE", "物理删除（DELETE）"),
    LOGICAL_DELETE_SIGN("LOGICAL_DELETE_SIGN", "逻辑删除（删除标记列）");

    private final String code;
    private final String label;

    DeleteSyncMode(String code, String label) {
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
