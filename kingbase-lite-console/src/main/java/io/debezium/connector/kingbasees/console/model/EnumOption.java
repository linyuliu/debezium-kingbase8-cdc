package io.debezium.connector.kingbasees.console.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 枚举选项项：用于前端下拉框展示。
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class EnumOption {

    private String code;
    private String label;
}
