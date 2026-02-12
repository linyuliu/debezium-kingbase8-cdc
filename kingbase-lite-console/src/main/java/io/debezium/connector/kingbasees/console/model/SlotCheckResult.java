package io.debezium.connector.kingbasees.console.model;

import lombok.Data;

/**
 * Kingbase 复制槽检查结果。
 */
@Data
public class SlotCheckResult {

    private boolean ok;
    private boolean exists;
    private String slotName;
    private String plugin;
    private Boolean active;
    private String message;
}
