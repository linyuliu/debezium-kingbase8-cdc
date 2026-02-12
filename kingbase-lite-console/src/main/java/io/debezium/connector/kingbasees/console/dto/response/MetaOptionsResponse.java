package io.debezium.connector.kingbasees.console.dto.response;

import io.debezium.connector.kingbasees.console.model.EnumOption;
import lombok.Data;

import java.util.List;

/**
 * 前端元数据选项集合。
 */
@Data
public class MetaOptionsResponse {

    private List<EnumOption> dataSourceTypes;
    private List<EnumOption> runModes;
    private List<EnumOption> routeModes;
    private List<EnumOption> outputModes;
    private List<EnumOption> deleteSyncModes;
    private List<EnumOption> deltaNullStrategies;
}
