package io.debezium.connector.kingbasees.console.controller;

import io.debezium.connector.kingbasees.console.dto.request.TaskUpsertRequest;
import io.debezium.connector.kingbasees.console.model.RunMode;
import io.debezium.connector.kingbasees.console.model.SyncTaskConfig;
import io.debezium.connector.kingbasees.console.model.TaskRuntimeInfo;
import io.debezium.connector.kingbasees.console.service.TaskRunnerService;
import io.debezium.connector.kingbasees.console.service.TaskService;
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
 * 同步任务管理接口：
 * 支持任务 CRUD、手工触发、停止、状态和日志查询。
 */
@RestController
@RequestMapping("/api/tasks")
public class TaskController {

    private final TaskService taskService;
    private final TaskRunnerService taskRunnerService;

    public TaskController(TaskService taskService, TaskRunnerService taskRunnerService) {
        this.taskService = taskService;
        this.taskRunnerService = taskRunnerService;
    }

    @GetMapping
    public ApiResponse<List<SyncTaskConfig>> list() {
        return ApiResponse.ok(taskService.list());
    }

    @PostMapping
    public ApiResponse<SyncTaskConfig> create(@RequestBody TaskUpsertRequest body) {
        return ApiResponse.ok(taskService.create(body.toModel()));
    }

    @PutMapping("/{id}")
    public ApiResponse<SyncTaskConfig> update(@PathVariable("id") String id, @RequestBody TaskUpsertRequest body) {
        return ApiResponse.ok(taskService.update(id, body.toModel()));
    }

    @DeleteMapping("/{id}")
    public ApiResponse<String> delete(@PathVariable("id") String id) {
        taskService.delete(id);
        return ApiResponse.ok("deleted");
    }

    @PostMapping("/{id}/run")
    public ApiResponse<TaskRuntimeInfo> run(@PathVariable("id") String id,
                                            @RequestParam(value = "mode", required = false, defaultValue = "RESUME_CDC") String mode) {
        SyncTaskConfig task = taskService.getRequired(id);
        return ApiResponse.ok(taskRunnerService.runTask(task, RunMode.fromText(mode), "manual"));
    }

    @PostMapping("/{id}/stop")
    public ApiResponse<TaskRuntimeInfo> stop(@PathVariable("id") String id) {
        return ApiResponse.ok(taskRunnerService.stopTask(id));
    }

    @GetMapping("/{id}/runtime")
    public ApiResponse<TaskRuntimeInfo> runtime(@PathVariable("id") String id) {
        return ApiResponse.ok(taskRunnerService.runtime(id));
    }

    @GetMapping("/{id}/logs")
    public ApiResponse<List<String>> logs(@PathVariable("id") String id,
                                          @RequestParam(value = "tail", required = false) Integer tail) {
        return ApiResponse.ok(taskRunnerService.logs(id, tail));
    }

    @GetMapping("/runtime/all")
    public ApiResponse<List<TaskRuntimeInfo>> allRuntime() {
        return ApiResponse.ok(taskRunnerService.listAllRuntime());
    }
}
