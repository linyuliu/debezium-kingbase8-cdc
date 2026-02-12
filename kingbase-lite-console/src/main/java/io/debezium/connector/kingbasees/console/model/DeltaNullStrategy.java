package io.debezium.connector.kingbasees.console.model;

public enum DeltaNullStrategy {
    SKIP("SKIP", "空值跳过"),
    ZERO("ZERO", "空值按 0 参与计算");

    private final String code;
    private final String label;

    DeltaNullStrategy(String code, String label) {
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
