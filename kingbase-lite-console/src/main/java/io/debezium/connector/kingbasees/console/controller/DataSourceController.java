package io.debezium.connector.kingbasees.console.controller;

import io.debezium.connector.kingbasees.console.dto.request.DataSourceUpsertRequest;
import io.debezium.connector.kingbasees.console.model.DataSourceConfig;
import io.debezium.connector.kingbasees.console.model.DataSourceTestResult;
import io.debezium.connector.kingbasees.console.model.SlotCheckResult;
import io.debezium.connector.kingbasees.console.service.DataSourceService;
import io.debezium.connector.kingbasees.console.web.ApiResponse;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * 数据源管理接口：
 * 提供数据源 CRUD、连接测试、复制槽检查。
 */
@RestController
@RequestMapping("/api/datasources")
public class DataSourceController {

    private final DataSourceService dataSourceService;

    public DataSourceController(DataSourceService dataSourceService) {
        this.dataSourceService = dataSourceService;
    }

    @GetMapping
    public ApiResponse<List<DataSourceConfig>> list() {
        return ApiResponse.ok(dataSourceService.list());
    }

    @PostMapping
    public ApiResponse<DataSourceConfig> create(@RequestBody DataSourceUpsertRequest body) {
        return ApiResponse.ok(dataSourceService.create(body.toModel()));
    }

    @PutMapping("/{id}")
    public ApiResponse<DataSourceConfig> update(@PathVariable("id") String id, @RequestBody DataSourceUpsertRequest body) {
        return ApiResponse.ok(dataSourceService.update(id, body.toModel()));
    }

    @DeleteMapping("/{id}")
    public ApiResponse<String> delete(@PathVariable("id") String id) {
        dataSourceService.delete(id);
        return ApiResponse.ok("deleted");
    }

    @PostMapping("/{id}/test")
    public ApiResponse<DataSourceTestResult> test(@PathVariable("id") String id) {
        return ApiResponse.ok(dataSourceService.testConnectionById(id));
    }

    @PostMapping("/{id}/slot-check")
    public ApiResponse<SlotCheckResult> checkSlot(@PathVariable("id") String id,
                                                  @RequestParam("slotName") String slotName) {
        return ApiResponse.ok(dataSourceService.checkSlot(id, slotName));
    }
}
