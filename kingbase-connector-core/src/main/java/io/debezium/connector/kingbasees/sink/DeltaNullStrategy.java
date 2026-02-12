package io.debezium.connector.kingbasees.sink;

/**
 * 数值增量计算时，遇到空值的策略。
 */
enum DeltaNullStrategy {
    SKIP("skip"),
    ZERO("zero");

    private final String code;

    DeltaNullStrategy(String code) {
        this.code = code;
    }

    static DeltaNullStrategy fromCode(String text) {
        String value = SinkSupport.lower(text);
        if ("zero".equals(value) || "null_as_zero".equals(value) || "0".equals(value)) {
            return ZERO;
        }
        return SKIP;
    }

    String getCode() {
        return code;
    }
}
