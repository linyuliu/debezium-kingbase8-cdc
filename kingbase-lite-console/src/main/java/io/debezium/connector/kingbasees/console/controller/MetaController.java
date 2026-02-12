package io.debezium.connector.kingbasees.console.controller;

import io.debezium.connector.kingbasees.console.dto.response.MetaOptionsResponse;
import io.debezium.connector.kingbasees.console.model.DataSourceType;
import io.debezium.connector.kingbasees.console.model.DeleteSyncMode;
import io.debezium.connector.kingbasees.console.model.DeltaNullStrategy;
import io.debezium.connector.kingbasees.console.model.EnumOption;
import io.debezium.connector.kingbasees.console.model.OutputMode;
import io.debezium.connector.kingbasees.console.model.RouteMode;
import io.debezium.connector.kingbasees.console.model.RunMode;
import io.debezium.connector.kingbasees.console.web.ApiResponse;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

/**
 * 前端元数据接口：
 * 统一输出枚举 code/label，避免页面硬编码。
 */
@RestController
@RequestMapping("/api/meta")
public class MetaController {

    @GetMapping("/options")
    public ApiResponse<MetaOptionsResponse> options() {
        MetaOptionsResponse response = new MetaOptionsResponse();
        response.setDataSourceTypes(buildDataSourceTypeOptions());
        response.setRunModes(buildRunModeOptions());
        response.setRouteModes(buildRouteModeOptions());
        response.setOutputModes(buildOutputModeOptions());
        response.setDeleteSyncModes(buildDeleteSyncModeOptions());
        response.setDeltaNullStrategies(buildDeltaNullStrategyOptions());
        return ApiResponse.ok(response);
    }

    private List<EnumOption> buildDataSourceTypeOptions() {
        List<EnumOption> options = new ArrayList<EnumOption>();
        for (DataSourceType type : DataSourceType.values()) {
            options.add(new EnumOption(type.getCode(), type.getLabel()));
        }
        return options;
    }

    private List<EnumOption> buildRunModeOptions() {
        List<EnumOption> options = new ArrayList<EnumOption>();
        for (RunMode mode : RunMode.values()) {
            options.add(new EnumOption(mode.getCode(), mode.getLabel()));
        }
        return options;
    }

    private List<EnumOption> buildRouteModeOptions() {
        List<EnumOption> options = new ArrayList<EnumOption>();
        for (RouteMode mode : RouteMode.values()) {
            options.add(new EnumOption(mode.getCode(), mode.getLabel()));
        }
        return options;
    }

    private List<EnumOption> buildOutputModeOptions() {
        List<EnumOption> options = new ArrayList<EnumOption>();
        for (OutputMode mode : OutputMode.values()) {
            options.add(new EnumOption(mode.getCode(), mode.getLabel()));
        }
        return options;
    }

    private List<EnumOption> buildDeleteSyncModeOptions() {
        List<EnumOption> options = new ArrayList<EnumOption>();
        for (DeleteSyncMode mode : DeleteSyncMode.values()) {
            options.add(new EnumOption(mode.getCode(), mode.getLabel()));
        }
        return options;
    }

    private List<EnumOption> buildDeltaNullStrategyOptions() {
        List<EnumOption> options = new ArrayList<EnumOption>();
        for (DeltaNullStrategy strategy : DeltaNullStrategy.values()) {
            options.add(new EnumOption(strategy.getCode(), strategy.getLabel()));
        }
        return options;
    }
}
