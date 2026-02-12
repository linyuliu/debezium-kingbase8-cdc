package io.debezium.connector.kingbasees.console.model;

public enum RunMode {
    RESUME_CDC("RESUME_CDC", "断点续传+CDC"),
    FULL_THEN_CDC("FULL_THEN_CDC", "全量后CDC"),
    FORCE_FULL_THEN_CDC("FORCE_FULL_THEN_CDC", "强制全量后CDC"),
    CDC_ONLY("CDC_ONLY", "仅CDC");

    private final String code;
    private final String label;

    RunMode(String code, String label) {
        this.code = code;
        this.label = label;
    }

    public String getCode() {
        return code;
    }

    public String getLabel() {
        return label;
    }

    public static RunMode fromText(String text) {
        if (text == null || text.trim().isEmpty()) {
            return RESUME_CDC;
        }
        for (RunMode mode : values()) {
            if (mode.name().equalsIgnoreCase(text.trim()) || mode.code.equalsIgnoreCase(text.trim())) {
                return mode;
            }
        }
        throw new IllegalArgumentException("不支持的运行模式: " + text);
    }
}
